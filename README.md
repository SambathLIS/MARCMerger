
MARC Merger

MARC Merger is a Python-based GUI tool to merge MARC bibliographic records from Local and External sources. 
It provides a side-by-side preview of Local, External, and Merged records, supports gap-filling merge, 
and allows saving in MARC, Text, or MARCXML formats.

---

Features

- Load Local MARC and External MARC files (.mrc, .marc)
- Match records by ISBN (from 020 $a field, normalized)
- Show only matched records in the list
- Side-by-side formatted preview of:
  - Local record
  - External record (first occurrence)
  - Merged record
- Gap-filling merge: fills missing tags and subfields without overwriting existing data
- Preserve local 9XX fields while merging
- Optionally remove eBook-only fields (856, 347, 538, 007, 245 $h)
- Save options:
  - Selected record
  - All records
  - Merged records in .mrc, .txt, or .xml (if pymarc supports MARCXML)
- Dark/Light mode toggle for UI

---

Install dependencies:

pip install -r requirements.txt

---

Usage

Run the application:

python marc_merger.py

Steps to use:

1. Load Local MARC and External MARC files.
2. Click Start Merge to merge records.
3. Preview Local, External, and Merged records side by side.
4. Use Save Selected, Save All, or Save Merged Records to export data.
5. Toggle Dark/Light Mode using the moon/sun button.

---

Supported File Formats

- Input: MARC (.mrc, .marc)
- Output:
  - MARC (.mrc)
  - Text (.txt)
  - MARCXML (.xml) – optional if pymarc supports record_to_xml

---

Dependencies

- PyQt5 – for GUI
- pymarc – for MARC file handling
- lxml – optional, for MARCXML output

You can install all dependencies via:

pip install PyQt5 pymarc lxml

---

Contributing

Contributions are welcome! If you find bugs, want new features, or want to improve the tool, 
please open an issue or submit a pull request.

---

License

This project is licensed under the MIT License. See LICENSE for details.
