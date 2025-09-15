from flask import Flask, request, jsonify, send_file
from io import BytesIO
from pymarc import MARCReader, MARCWriter, Record, Field

app = Flask(__name__)

# Utility and merging functions adapted from your code

def normalize_isbn(isbn):
    if not isbn:
        return None
    s = str(isbn).split(' ')[0].split('(')[0].replace('-', '').replace(' ', '').strip()
    return s if s else None

def extract_isbns_from_record(rec):
    isbns = set()
    try:
        for f in rec.get_fields('020'):
            for a in f.get_subfields('a'):
                n = normalize_isbn(a)
                if n:
                    isbns.add(n)
    except Exception:
        pass
    return isbns

def _all_subfield_pairs(field):
    sf_list = getattr(field, 'subfields', None)
    if not sf_list:
        return []
    first = sf_list[0]
    if hasattr(first, 'code') and hasattr(first, 'value'):
        return [(sf.code, sf.value) for sf in sf_list]
    else:
        pairs = list(zip(sf_list[0::2], sf_list[1::2]))
        return pairs

def merge_fill_gaps(local_rec, external_rec, preserve_9xx=True):
    # Clone local record deeply
    try:
        merged = MARCReader(BytesIO(local_rec.as_marc())).__next__()
    except Exception:
        merged = Record()
        merged.leader = getattr(local_rec, 'leader', '')
        for f in local_rec.get_fields():
            merged.add_field(f)

    for f in external_rec.get_fields():
        if f.tag in ('001', '003', '005', '008'):
            continue
        if preserve_9xx and f.tag.startswith('9'):
            if any(lf.tag == f.tag for lf in merged.get_fields() if lf.tag.startswith('9')):
                continue

        local_fields = merged.get_fields(f.tag)
        if not local_fields:
            merged.add_field(f)
            continue

        ext_pairs = _all_subfield_pairs(f)
        if not ext_pairs:
            continue

        for lf in local_fields:
            try:
                lf_pairs = _all_subfield_pairs(lf)
                local_codes = [code for (code, val) in lf_pairs]
            except Exception:
                local_codes = []

            for (code, val) in ext_pairs:
                if code not in local_codes:
                    try:
                        lf.add_subfield(code, val)
                        local_codes.append(code)
                    except Exception:
                        try:
                            new_pairs = lf_pairs + [(c, v) for (c, v) in ext_pairs if c not in [x for x, _ in lf_pairs]]
                            flat = []
                            for (c, v) in new_pairs:
                                flat.append(c)
                                flat.append(v)
                            inds = getattr(lf, 'indicators', None)
                            if inds and len(inds) >= 2:
                                newf = Field(tag=lf.tag, indicators=inds, subfields=flat)
                            else:
                                ind1 = getattr(lf, 'indicator1', ' ')
                                ind2 = getattr(lf, 'indicator2', ' ')
                                newf = Field(tag=lf.tag, indicators=[ind1, ind2], subfields=flat)
                            merged.remove_field(lf)
                            merged.add_field(newf)
                            break
                        except Exception:
                            pass
    return merged

@app.route('/merge', methods=['POST'])
def merge_records():
    if 'local' not in request.files or 'external' not in request.files:
        return jsonify({'error': 'Upload both local and external MARC files as "local" and "external".'}), 400

    local_file = request.files['local']
    external_file = request.files['external']

    local_map = {}
    external_map = {}

    try:
        local_reader = MARCReader(local_file.stream)
        for rec in local_reader:
            isbns = extract_isbns_from_record(rec)
            for isbn in isbns:
                if isbn not in local_map:
                    local_map[isbn] = rec
    except Exception as e:
        return jsonify({'error': f'Error reading local MARC file: {str(e)}'}), 400

    try:
        external_reader = MARCReader(external_file.stream)
        for rec in external_reader:
            isbns = extract_isbns_from_record(rec)
            for isbn in isbns:
                external_map.setdefault(isbn, []).append(rec)
    except Exception as e:
        return jsonify({'error': f'Error reading external MARC file: {str(e)}'}), 400

    merged_records = []
    matched_isbns = local_map.keys() & external_map.keys()

    for isbn in matched_isbns:
        local_rec = local_map[isbn]
        ext_recs = external_map[isbn]
        merged = local_rec
        for ext_rec in ext_recs:
            merged = merge_fill_gaps(merged, ext_rec)
        merged_records.append(merged)

    if not merged_records:
        return jsonify({'error': 'No matching ISBNs found between local and external files.'}), 400

    memfile = BytesIO()
    writer = MARCWriter(memfile)
    for rec in merged_records:
        writer.write(rec)
    writer.close()
    memfile.seek(0)

    return send_file(memfile, mimetype='application/marc',
                     as_attachment=True, download_name='merged_records.mrc')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
