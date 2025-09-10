#!/usr/bin/env python3
"""
MARC Merger - Dashboard with 3-way preview (Local | External | Merged)
Features:
 - Load Local MARC and External MARC
 - Match by ISBN (020 $a normalized)
 - Show only matched records in list
 - Side-by-side formatted preview: Local, External (first), Merged
 - Background merge thread with progress + cancel
 - Save Selected / Save All / Save Merged (MARC .mrc or Text .txt). Attempts MARCXML if pymarc provides record_to_xml.
 - Gap-filling merge: add missing tags & missing subfields only (no overwrites)
"""
import sys
import os
import traceback
from io import BytesIO
from functools import partial

from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QFileDialog, QProgressBar, QListWidget,
    QPlainTextEdit, QMessageBox, QSplitter, QFrame, QToolButton, QSizePolicy,
    QComboBox
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont

from pymarc import MARCReader, Field, MARCWriter, Record

# Attempt to import pymarc record_to_xml helper (may exist depending on version)
try:
    from pymarc import record_to_xml
    HAVE_RECORD_TO_XML = True
except Exception:
    HAVE_RECORD_TO_XML = False

# ---------------- Utility functions ----------------

def normalize_isbn(isbn):
    """Normalize ISBN-like token: remove hyphens/spaces, strip qualifiers."""
    if not isbn:
        return None
    s = str(isbn)
    # take first token and remove parentheses qualifiers
    s = s.split(' ')[0]
    s = s.split('(')[0]
    s = s.replace('-', '').replace(' ', '')
    s = s.strip()
    return s if s else None


def extract_isbns_from_record(rec):
    """Return set of normalized ISBNs from 020 $a subfields."""
    isbns = set()
    try:
        for f in rec.get_fields('020'):
            # some pymarc shapes allow get_subfields('a') or subfields list
            try:
                for a in f.get_subfields('a'):
                    n = normalize_isbn(a)
                    if n:
                        isbns.add(n)
            except Exception:
                # fallback to raw subfields parsing
                subs = getattr(f, 'subfields', []) or []
                for code, val in zip(subs[0::2], subs[1::2]):
                    if code == 'a':
                        n = normalize_isbn(val)
                        if n:
                            isbns.add(n)
    except Exception:
        pass
    return isbns


def _format_data_field(field):
    """
    Format a data field to MarcEdit-like string, e.g.
    =245  14 $aTitle :$bSubtitle
    Uses '\' to represent blank indicators as MarcEdit does (\\ when both blank).
    Handles both flat subfields list and Subfield objects.
    """
    # Determine indicators robustly
    ind1 = ind2 = '\\'  # MarcEdit shows '\' for blank indicator
    inds = getattr(field, 'indicators', None)
    if inds and len(inds) >= 2:
        # pymarc may use ' ' for blank; convert to backslash
        ind1 = inds[0] if inds[0] != ' ' and inds[0] is not None else '\\'
        ind2 = inds[1] if inds[1] != ' ' and inds[1] is not None else '\\'
    else:
        ind1 = getattr(field, 'indicator1', None)
        ind2 = getattr(field, 'indicator2', None)
        ind1 = ind1 if ind1 not in (None, ' ') else '\\'
        ind2 = ind2 if ind2 not in (None, ' ') else '\\'

    # Build subfields string
    subs = ''
    sf_list = getattr(field, 'subfields', None)
    if sf_list:
        # two possible pymarc shapes:
        # 1) flat list: ['a','Title','b','Subtitle']
        # 2) list of Subfield objects: [Subfield(code='a', value='Title'), ...]
        # Detect by inspecting first element
        first = sf_list[0]
        if hasattr(first, 'code') and hasattr(first, 'value'):
            # Subfield objects
            try:
                subs = ''.join(f"${sf.code}{sf.value}" for sf in sf_list)
            except Exception:
                # fallback to string conversion
                subs = ''.join(f"${getattr(sf, 'code', '')}{getattr(sf, 'value', '')}" for sf in sf_list)
        else:
            # flat list
            try:
                pairs = list(zip(sf_list[0::2], sf_list[1::2]))
                subs = ''.join(f"${code}{val}" for code, val in pairs)
            except Exception:
                # final fallback: str join
                subs = ''.join(str(x) for x in sf_list)
    return f"={field.tag}  {ind1}{ind2} {subs}"


def _format_control_field(field):
    """Format control field like =001  data"""
    data = getattr(field, 'data', '') or ''
    return f"={field.tag}  {data}"


def record_to_pretty_text(rec):
    """
    Format a pymarc.Record into MarcEdit-like tree text:
    =LDR  <leader>
    =001  <data>
    =020  \\$a...
    Fields are sorted numerically by MARC tag
    """
    if rec is None:
        return "(no record)"

    lines = []
    try:
        leader = rec.leader if hasattr(rec, 'leader') else ''
        lines.append(f"=LDR  {leader}")
    except Exception:
        pass

    # sort fields numerically (control fields first)
    try:
        fields_sorted = sorted(rec.get_fields(), key=lambda f: int(f.tag) if f.tag.isdigit() else 9999)
    except Exception:
        # fallback to original order
        fields_sorted = list(rec.get_fields())

    for field in fields_sorted:
        try:
            tag = field.tag
        except Exception:
            continue

        # check if control field
        try:
            is_control = getattr(field, 'is_control_field', lambda: False)()
        except Exception:
            is_control = False

        try:
            tag_num = int(tag)
        except Exception:
            tag_num = 1000

        if is_control or tag_num < 10:
            lines.append(_format_control_field(field))
        else:
            lines.append(_format_data_field(field))

    return '\n'.join(lines)


# ---------------- Gap-filling merge helper ----------------

def _all_subfield_pairs(field):
    """
    Return list of (code, value) pairs for a field.
    Works for both flat subfields list and Subfield objects.
    """
    sf_list = getattr(field, 'subfields', None)
    if not sf_list:
        return []
    first = sf_list[0]
    if hasattr(first, 'code') and hasattr(first, 'value'):
        return [(sf.code, sf.value) for sf in sf_list]
    else:
        pairs = list(zip(sf_list[0::2], sf_list[1::2]))
        return pairs


def merge_fill_gaps(local_rec, external_rec, options=None):
    """
    Merge external_rec into local_rec by filling gaps:
     - if a tag missing in local -> copy entire external field
     - if tag exists in local -> add missing subfields to each local occurrence
    Returns a new Record (deep clone of local + additions).
    """
    options = options or {}
    preserve_9xx = options.get('preserve_9xx', True)

    # clone local safely
    try:
        merged = MARCReader(BytesIO(local_rec.as_marc())).__next__()
    except Exception:
        # fallback: shallow copy via Record()
        merged = Record()
        merged.leader = getattr(local_rec, 'leader', '')

        # try to copy fields
        try:
            for f in local_rec.get_fields():
                merged.add_field(f)
        except Exception:
            pass

    # iterate external fields
    for f in external_rec.get_fields():
        # skip system / certain control fields
        if f.tag in ('001', '003', '005', '008'):
            continue
        # respect preserve_9xx option: don't copy 9XX from external if local has same tag
        if preserve_9xx and f.tag.startswith('9'):
            # if any local 9xx present for the same tag, skip external copy
            if any(lf.tag == f.tag for lf in merged.get_fields() if lf.tag.startswith('9')):
                continue

        # find local fields with same tag
        local_fields = merged.get_fields(f.tag)
        if not local_fields:
            # tag missing entirely -> copy entire field
            merged.add_field(f)
            continue

        # tag exists -> fill missing subfields for each local field occurrence
        # We'll compare subfield codes per local field and add missing codes
        ext_pairs = _all_subfield_pairs(f)
        if not ext_pairs:
            continue

        # build an index of local subfield codes present per local field
        for lf in local_fields:
            try:
                lf_pairs = _all_subfield_pairs(lf)
                local_codes = [code for (code, val) in lf_pairs]
            except Exception:
                local_codes = []

            for (code, val) in ext_pairs:
                if code not in local_codes:
                    # add missing subfield to this local field
                    try:
                        # pymarc Field.add_subfield(code, value)
                        lf.add_subfield(code, val)
                        # update local_codes to avoid duplicate adds on same field
                        local_codes.append(code)
                    except Exception:
                        # fallback: attempt to recreate field with extended subfields
                        try:
                            # create new subfields list merging unique codes
                            new_pairs = lf_pairs + [(c, v) for (c, v) in ext_pairs if c not in [x for x, _ in lf_pairs]]
                            flat = []
                            for (c, v) in new_pairs:
                                flat.append(c); flat.append(v)
                            # create new Field preserving indicators if possible
                            inds = getattr(lf, 'indicators', None)
                            if inds and len(inds) >= 2:
                                newf = Field(tag=lf.tag, indicators=inds, subfields=flat)
                            else:
                                ind1 = getattr(lf, 'indicator1', ' ')
                                ind2 = getattr(lf, 'indicator2', ' ')
                                newf = Field(tag=lf.tag, indicators=[ind1, ind2], subfields=flat)
                            # replace lf with newf
                            merged.remove_field(lf)
                            merged.add_field(newf)
                            break  # field replaced; break inner loop
                        except Exception:
                            pass

    return merged


# ---------------- Worker Thread ----------------

class MergeWorker(QThread):
    progress = pyqtSignal(int)
    current = pyqtSignal(str)
    finished = pyqtSignal(list, dict, dict, dict)  # merged_list, stats, local_map, external_map
    error = pyqtSignal(str)

    def __init__(self, local_path, external_path, options=None):
        super().__init__()
        self.local_path = local_path
        self.external_path = external_path
        self.options = options or {}
        self._stop_requested = False

    def request_stop(self):
        self._stop_requested = True

    def run(self):
        try:
            self.current.emit("Loading local records...")
            local_map = {}     # isbn -> record (first occurrence)
            with open(self.local_path, 'rb') as fh:
                reader = MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling='replace')
                for i, rec in enumerate(reader):
                    if self._stop_requested:
                        self.error.emit("Cancelled.")
                        return
                    isbns = extract_isbns_from_record(rec)
                    if isbns:
                        for isbn in isbns:
                            if isbn not in local_map:
                                local_map[isbn] = rec
                    else:
                        # use special key for no-isbn local (not included in matched list since no external match)
                        local_map[f"LOCAL_NOISBN_{i}"] = rec

            self.current.emit("Loading external records...")
            external_map = {}  # isbn -> list(records)
            with open(self.external_path, 'rb') as fh:
                reader = MARCReader(fh, to_unicode=True, force_utf8=True, utf8_handling='replace')
                for i, rec in enumerate(reader):
                    if self._stop_requested:
                        self.error.emit("Cancelled.")
                        return
                    isbns = extract_isbns_from_record(rec)
                    if isbns:
                        for isbn in isbns:
                            external_map.setdefault(isbn, []).append(rec)
                    else:
                        external_map.setdefault(f"EXT_NOISBN_{i}", []).append(rec)

            # Only include keys that exist in both maps (matched by ISBN)
            matched_keys = sorted([k for k in local_map.keys() if k in external_map])
            total = max(1, len(matched_keys))
            merged_list = []
            stats = {
                'local_loaded': len(local_map),
                'external_loaded': sum(len(v) for v in external_map.values()),
                'matched': len(matched_keys)
            }

            for idx, key in enumerate(matched_keys):
                if self._stop_requested:
                    self.error.emit("Cancelled.")
                    return
                self.current.emit(f"Merging {idx+1}/{total} ({key})")
                pct = int((idx / total) * 100)
                self.progress.emit(pct)

                local_rec = local_map.get(key)
                ext_recs = external_map.get(key, [])

                # Create merged copy of local as base
                try:
                    base = MARCReader(BytesIO(local_rec.as_marc())).__next__()
                except Exception:
                    # fallback shallow clone
                    base = Record()
                    base.leader = getattr(local_rec, 'leader', '')
                    for f in local_rec.get_fields():
                        base.add_field(f)

                # For each external record for this ISBN, fill gaps into base sequentially
                for ext in ext_recs:
                    if self._stop_requested:
                        self.error.emit("Cancelled.")
                        return

                    # Optionally remove ebook-only fields from ext copy
                    ext_copy = None
                    try:
                        ext_copy = MARCReader(BytesIO(ext.as_marc())).__next__()
                    except Exception:
                        # fallback use ext reference (risky) but continue
                        ext_copy = ext

                    if self.options.get('remove_ebook_fields', True):
                        try:
                            for tag in ('856', '347', '538'):
                                for f in list(ext_copy.get_fields(tag)):
                                    ext_copy.remove_field(f)
                            for f in list(ext_copy.get_fields('007')):
                                if getattr(f, 'data', None) and len(f.data) > 0 and f.data[0].lower() in ('o', 'c'):
                                    ext_copy.remove_field(f)
                            # remove electronic notes in 245 $h
                            for f in list(ext_copy.get_fields('245')):
                                subpairs = list(zip(f.subfields[0::2], f.subfields[1::2])) if f.subfields else []
                                new_subs = []
                                for c, v in subpairs:
                                    if c == 'h' and ('electronic resource' in v.lower() or 'online resource' in v.lower()):
                                        continue
                                    new_subs.extend([c, v])
                                if new_subs:
                                    try:
                                        newf = Field(tag='245', indicators=[f.indicator1, f.indicator2], subfields=new_subs)
                                    except Exception:
                                        inds = getattr(f, 'indicators', [' ', ' '])
                                        newf = Field(tag='245', indicators=inds, subfields=new_subs)
                                    ext_copy.remove_field(f)
                                    ext_copy.add_field(newf)
                                else:
                                    ext_copy.remove_field(f)
                        except Exception:
                            # non-critical; proceed
                            pass

                    # apply gap-filling merge from ext_copy into base
                    try:
                        base = merge_fill_gaps(base, ext_copy, options=self.options)
                    except Exception:
                        # if merge_fill_gaps fails, continue with what we have
                        pass

                merged_list.append((key, base))

            # finished
            self.progress.emit(100)
            self.current.emit("Done")
            # emit merged_list in simple form: list of Records (ordered same as matched_keys)
            merged_records = [r for (k, r) in merged_list]
            # Convert merged_list to dict for retrieval by key
            merged_map = {k: r for (k, r) in merged_list}
            # send finished -> merged_map keys list order follows matched_keys
            self.finished.emit([merged_map[k] for k in matched_keys], stats, local_map, external_map)
        except Exception as e:
            tb = traceback.format_exc()
            self.error.emit(str(e) + "\n" + tb)


# ---------------- GUI ----------------

class MarcMergerDashboard(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("MARC Merger - Dashboard (3-way preview)")
        self.resize(1400, 820)

        # paths and maps
        self.local_path = None
        self.external_path = None
        self.local_map = {}
        self.external_map = {}
        self.merged_records = []  # list of merged records in matched_keys order
        self.matched_keys = []

        # UI building
        self._build_ui()
        self._apply_light_style()

        self.worker = None

    def _build_ui(self):
        root = QVBoxLayout(self)

        # Header row: title and buttons
        header = QHBoxLayout()
        title = QLabel("MARC Merger")
        title.setStyleSheet("font-weight:700; font-size:20px;")
        title.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        header.addWidget(title)

        self.btn_load_local = QPushButton("Load Local MARC")
        self.btn_load_local.clicked.connect(partial(self.select_file, 'local'))
        self.btn_load_external = QPushButton("Load External MARC")
        self.btn_load_external.clicked.connect(partial(self.select_file, 'external'))
        header.addWidget(self.btn_load_local)
        header.addWidget(self.btn_load_external)

        # Save merged button
        self.btn_save_merged = QPushButton("Save Merged Records")
        self.btn_save_merged.clicked.connect(self.save_merged_records)
        header.addWidget(self.btn_save_merged)

        self.dark_toggle = QToolButton()
        self.dark_toggle.setCheckable(True)
        self.dark_toggle.setText("🌙 Dark Mode")
        self.dark_toggle.clicked.connect(self._toggle_dark)
        header.addWidget(self.dark_toggle)

        root.addLayout(header)

        # Subheader: file names
        files_h = QHBoxLayout()
        self.local_label = QLabel("(local: none)")
        self.external_label = QLabel("(external: none)")
        files_h.addWidget(self.local_label)
        files_h.addStretch(1)
        files_h.addWidget(self.external_label)
        root.addLayout(files_h)

        # Main splitter: left list, right previews area
        splitter = QSplitter(Qt.Horizontal)
        splitter.setHandleWidth(8)

        # Left frame (Record list + controls)
        left_frame = QFrame()
        left_layout = QVBoxLayout(left_frame)
        left_layout.setContentsMargins(6, 6, 6, 6)
        left_layout.setSpacing(8)

        header_row = QHBoxLayout()
        header_row.addWidget(QLabel("Record List (Matched ISBNs)"))
        self.lbl_matches = QLabel("Matched: 0")
        header_row.addStretch(1)
        header_row.addWidget(self.lbl_matches)
        left_layout.addLayout(header_row)

        self.record_list = QListWidget()
        self.record_list.currentRowChanged.connect(self.on_record_selected)
        left_layout.addWidget(self.record_list, 1)

        controls = QHBoxLayout()
        self.btn_merge = QPushButton("Start Merge")
        self.btn_merge.clicked.connect(self.start_merge)
        self.btn_cancel = QPushButton("Cancel")
        self.btn_cancel.clicked.connect(self.cancel_merge)
        self.btn_cancel.setEnabled(False)
        controls.addWidget(self.btn_merge)
        controls.addWidget(self.btn_cancel)
        left_layout.addLayout(controls)

        self.progress = QProgressBar()
        self.progress.setValue(0)
        left_layout.addWidget(self.progress)

        # export buttons
        export_row = QHBoxLayout()
        self.export_format = QComboBox()
        # Offer formats we support
        self.export_format.addItem("MARC (.mrc)", "mrc")
        if HAVE_RECORD_TO_XML:
            self.export_format.addItem("MARCXML (.xml)", "xml")
        self.export_format.addItem("Text (.txt)", "txt")
        export_row.addWidget(QLabel("Export format:"))
        export_row.addWidget(self.export_format)
        export_row.addStretch(1)
        self.btn_save_selected = QPushButton("Save Selected")
        self.btn_save_selected.clicked.connect(self.save_selected)
        self.btn_save_all = QPushButton("Save All")
        self.btn_save_all.clicked.connect(self.save_all)
        export_row.addWidget(self.btn_save_selected)
        export_row.addWidget(self.btn_save_all)
        left_layout.addLayout(export_row)

        splitter.addWidget(left_frame)

        # Right frame: three previews side-by-side
        right_frame = QFrame()
        right_layout = QVBoxLayout(right_frame)
        right_layout.setContentsMargins(6, 6, 6, 6)
        right_layout.setSpacing(6)

        # Top labels row
        labels_row = QHBoxLayout()
        labels_row.addWidget(QLabel("Local Record"), 1)
        labels_row.addWidget(QLabel("External Record"), 1)
        labels_row.addWidget(QLabel("Merged Record"), 1)
        right_layout.addLayout(labels_row)

        # Previews
        previews_row = QHBoxLayout()
        self.local_preview = QPlainTextEdit()
        self.local_preview.setReadOnly(True)
        self.external_preview = QPlainTextEdit()
        self.external_preview.setReadOnly(True)
        self.merged_preview = QPlainTextEdit()
        self.merged_preview.setReadOnly(True)

        # Monospaced font for clarity
        mono = QFont("Courier New", 12)
        self.local_preview.setFont(mono)
        self.external_preview.setFont(mono)
        self.merged_preview.setFont(mono)

        previews_row.addWidget(self.local_preview, 1)
        previews_row.addWidget(self.external_preview, 1)
        previews_row.addWidget(self.merged_preview, 1)
        right_layout.addLayout(previews_row, stretch=1)

        splitter.addWidget(right_frame)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 3)

        root.addWidget(splitter, stretch=1)

    # ---------------- Styles ----------------
    def _apply_light_style(self):
        self.setStyleSheet("""
            QWidget { background: #F6FBFF; font-family: Lato; font-size: 16px; color: #003cb3; }
            QPushButton { background-color: #1E88E5; color: white; padding: 8px 10px; border-radius: 6px; }
            QPushButton:disabled { background-color: #A7C8E8; color:#EAF2FF; }
            QProgressBar { border:1px solid #90CAF9; border-radius:6px; text-align:center; background:#EAF4FF; }
            QProgressBar::chunk { background-color:#1E88E5; border-radius:6px; }
            QPlainTextEdit { background: white; border: 1px solid #DDEFFB; border-radius:6px; padding:8px; }
            QListWidget { background: white; border: 1px solid #DDEFFB; border-radius:6px; padding:6px; }
            QLabel { font-weight:700; }
        """)
        self.dark_toggle.setText("🌙 Dark")

    def _apply_dark_style(self):
        self.setStyleSheet("""
            QWidget { background: #0F1417; font-family: Arial; font-size: 13px; color: #E6EEF6; }
            QPushButton { background-color: #1a1aff; color: white; padding: 8px 10px; border-radius: 6px; }
            QPushButton:disabled { background-color: #3A4A5A; color:#BFCBD6; }
            QProgressBar { border:1px solid #263238; border-radius:6px; text-align:center; background:#111316; }
            QProgressBar::chunk { background-color:#1a1aff; border-radius:6px; }
            QPlainTextEdit { background: #0B1113; border: 1px solid #22272A; border-radius:6px; padding:8px; color: #E6EEF6; }
            QListWidget { background: #0B1113; border: 1px solid #22272A; border-radius:6px; color: #E6EEF6; }
            QLabel { font-weight:600; }
        """)
        self.dark_toggle.setText("☀️ Light")

    def _toggle_dark(self):
        if self.dark_toggle.isChecked():
            self._apply_dark_style()
        else:
            self._apply_light_style()

    # ---------------- File selection ----------------

    def select_file(self, which):
        filt = "MARC files (*.mrc *.marc);;All files (*)"
        path, _ = QFileDialog.getOpenFileName(self, "Select MARC file", os.getcwd(), filt)
        if not path:
            return
        if which == 'local':
            self.local_path = path
            self.local_label.setText(f"Local: {os.path.basename(path)}")
        else:
            self.external_path = path
            self.external_label.setText(f"External: {os.path.basename(path)}")

    # ---------------- Merge control ----------------

    def start_merge(self):
        if not getattr(self, 'local_path', None) or not getattr(self, 'external_path', None):
            QMessageBox.warning(self, "Missing files", "Please load both Local and External MARC files.")
            return
        options = {
            'preserve_9xx': True,
            'remove_ebook_fields': True
        }
        self.btn_merge.setEnabled(False)
        self.btn_cancel.setEnabled(True)
        self.progress.setValue(0)
        self.local_map.clear()
        self.external_map.clear()
        self.merged_records.clear()
        self.matched_keys.clear()
        self.record_list.clear()
        self.local_preview.clear()
        self.external_preview.clear()
        self.merged_preview.clear()
        # start worker
        self.worker = MergeWorker(self.local_path, self.external_path, options)
        self.worker.progress.connect(self.progress.setValue)
        self.worker.current.connect(lambda txt: self.progress.setFormat(txt))
        self.worker.finished.connect(self.on_merge_finished)
        self.worker.error.connect(self.on_merge_error)
        self.worker.start()

    def cancel_merge(self):
        if self.worker:
            self.worker.request_stop()
            self.progress.setFormat("Cancelling...")

    def on_merge_finished(self, merged_list, stats, local_map, external_map):
        # merged_list is a list of records in order of matched_keys as implemented
        self.local_map = local_map
        self.external_map = external_map

        # Determine matched keys (only those present in both maps)
        self.matched_keys = sorted([k for k in local_map.keys() if k in external_map])
        self.merged_records = merged_list  # parallel to matched_keys (same ordering as worker)
        # Update UI list
        self.record_list.clear()
        for i, key in enumerate(self.matched_keys):
            rec = self.local_map.get(key)
            title = ""
            if rec:
                try:
                    for f in rec.get_fields('245'):
                        a = f.get_subfields('a')
                        if a:
                            title = a[0]
                            break
                except Exception:
                    pass
            isbns = key
            self.record_list.addItem(f"{i+1}. {title[:80]} [{isbns}]")
        self.lbl_matches.setText(f"Matched: {len(self.matched_keys)}")
        self.btn_merge.setEnabled(True)
        self.btn_cancel.setEnabled(False)
        self.progress.setValue(100)
        self.progress.setFormat("Done")
        QMessageBox.information(self, "Merge complete", f"Merge finished. Matched records: {len(self.matched_keys)}")

    def on_merge_error(self, msg):
        self.btn_merge.setEnabled(True)
        self.btn_cancel.setEnabled(False)
        self.progress.setFormat("Error/Cancelled")
        QMessageBox.critical(self, "Merge Error", msg)

    # ---------------- Selection & Preview ----------------

    def on_record_selected(self, row):
        if row < 0 or row >= len(self.matched_keys):
            self.local_preview.clear()
            self.external_preview.clear()
            self.merged_preview.clear()
            return
        key = self.matched_keys[row]
        local_rec = self.local_map.get(key)
        external_recs = self.external_map.get(key, [])
        merged_rec = self.merged_records[row] if row < len(self.merged_records) else None

        # Local preview
        if local_rec:
            try:
                txt = record_to_pretty_text(local_rec)
            except Exception:
                txt = str(local_rec)
            self.local_preview.setPlainText(txt)
        else:
            self.local_preview.setPlainText("(no local record)")

        # External preview (show first external record for that key)
        if external_recs:
            ext = external_recs[0]
            try:
                txt = record_to_pretty_text(ext)
            except Exception:
                txt = str(ext)
            self.external_preview.setPlainText(txt)
        else:
            self.external_preview.setPlainText("(no external record)")

        # Merged preview
        if merged_rec:
            try:
                txt = record_to_pretty_text(merged_rec)
            except Exception:
                txt = str(merged_rec)
            self.merged_preview.setPlainText(txt)
        else:
            self.merged_preview.setPlainText("(no merged record)")

    # ---------------- Export ----------------

    def save_selected(self):
        row = self.record_list.currentRow()
        if row < 0 or row >= len(self.matched_keys):
            QMessageBox.warning(self, "No selection", "Please select a matched record from the list.")
            return
        rec = self.merged_records[row]
        fmt = self.export_format.currentData()
        default_name = f"record_{row+1}"
        if fmt == 'mrc':
            path, _ = QFileDialog.getSaveFileName(self, "Save Selected Record (MARC)", os.getcwd(),
                                                 f"{default_name}.mrc", "MARC file (*.mrc)")
            if not path:
                return
            try:
                with open(path, 'wb') as fh:
                    writer = MARCWriter(fh)
                    writer.write(rec)
                    writer.close()
                QMessageBox.information(self, "Saved", f"Saved selected record to {path}")
            except Exception as e:
                QMessageBox.critical(self, "Save error", str(e))
        elif fmt == 'txt':
            path, _ = QFileDialog.getSaveFileName(self, "Save Selected Record (Text)", os.getcwd(),
                                                 f"{default_name}.txt", "Text file (*.txt)")
            if not path:
                return
            try:
                with open(path, 'w', encoding='utf-8') as fh:
                    fh.write(record_to_pretty_text(rec))
                QMessageBox.information(self, "Saved", f"Saved selected record to {path}")
            except Exception as e:
                QMessageBox.critical(self, "Save error", str(e))
        elif fmt == 'xml' and HAVE_RECORD_TO_XML:
            path, _ = QFileDialog.getSaveFileName(self, "Save Selected Record (MARCXML)", os.getcwd(),
                                                 f"{default_name}.xml", "XML file (*.xml)")
            if not path:
                return
            try:
                xml = record_to_xml(rec)
                with open(path, 'w', encoding='utf-8') as fh:
                    fh.write(xml)
                QMessageBox.information(self, "Saved", f"Saved selected record to {path}")
            except Exception as e:
                QMessageBox.critical(self, "Save error", str(e))
        else:
            QMessageBox.warning(self, "Unsupported", "Selected export format is not supported in this build.")

    def save_all(self):
        if not self.merged_records:
            QMessageBox.warning(self, "No records", "No merged records to save.")
            return
        fmt = self.export_format.currentData()
        default_name = "merged_records"
        if fmt == 'mrc':
            path, _ = QFileDialog.getSaveFileName(self, "Save All Merged Records (MARC)", os.getcwd(),
                                                 f"{default_name}.mrc", "MARC file (*.mrc)")
            if not path:
                return
            try:
                with open(path, 'wb') as fh:
                    writer = MARCWriter(fh)
                    for rec in self.merged_records:
                        writer.write(rec)
                    writer.close()
                QMessageBox.information(self, "Saved", f"Saved {len(self.merged_records)} records to {path}")
            except Exception as e:
                QMessageBox.critical(self, "Save error", str(e))
        elif fmt == 'txt':
            path, _ = QFileDialog.getSaveFileName(self, "Save All Merged Records (Text)", os.getcwd(),
                                                 f"{default_name}.txt", "Text file (*.txt)")
            if not path:
                return
            try:
                with open(path, 'w', encoding='utf-8') as fh:
                    for i, rec in enumerate(self.merged_records, start=1):
                        fh.write(f"=== Record {i} ===\n")
                        fh.write(record_to_pretty_text(rec))
                        fh.write("\n\n")
                QMessageBox.information(self, "Saved", f"Saved {len(self.merged_records)} records to {path}")
            except Exception as e:
                QMessageBox.critical(self, "Save error", str(e))
        elif fmt == 'xml' and HAVE_RECORD_TO_XML:
            path, _ = QFileDialog.getSaveFileName(self, "Save All Merged Records (MARCXML)", os.getcwd(),
                                                 f"{default_name}.xml", "XML file (*.xml)")
            if not path:
                return
            try:
                # create a simple MARCXML collection
                with open(path, 'w', encoding='utf-8') as fh:
                    fh.write('<?xml version="1.0" encoding="UTF-8"?>\n<collection xmlns="http://www.loc.gov/MARC21/slim">\n')
                    for rec in self.merged_records:
                        fh.write(record_to_xml(rec))
                        fh.write("\n")
                    fh.write('</collection>\n')
                QMessageBox.information(self, "Saved", f"Saved {len(self.merged_records)} records to {path}")
            except Exception as e:
                QMessageBox.critical(self, "Save error", str(e))
        else:
            QMessageBox.warning(self, "Unsupported", "Selected export format is not supported in this build.")

    def save_merged_records(self):
        """Save the merged records currently in memory (all matched)."""
        if not self.merged_records:
            QMessageBox.warning(self, "No merged records", "Run merge first to create merged records.")
            return
        path, _ = QFileDialog.getSaveFileName(self, "Save Merged Records (MARC)", os.getcwd(),
                                             "Merged records (*.mrc);;All files (*)")
        if not path:
            return
        try:
            with open(path, 'wb') as fh:
                writer = MARCWriter(fh)
                for rec in self.merged_records:
                    writer.write(rec)
                writer.close()
            QMessageBox.information(self, "Saved", f"Saved {len(self.merged_records)} merged records to {path}")
        except Exception as e:
            QMessageBox.critical(self, "Save error", str(e))


# ---------------- Main ----------------

def main():
    app = QApplication(sys.argv)
    win = MarcMergerDashboard()
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
