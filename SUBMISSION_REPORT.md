# Bao cao nop bai

## Ket qua pipeline

- Nguon du lieu: PDF, Transcript/Video, HTML, CSV, LegacyCode.
- File dau ra: `processed_knowledge_base.json`.
- Bao cao SLA: `pipeline_report.json`.
- Lan chay gan nhat: 28 document duoc xu ly, 28 document dat quality gate, 0 document bi loai.
- CSV record `id=7` co gia goc am `-350000` da duoc sua thanh `350000` va ghi metadata `price_correction_applied: negative_to_absolute`.
- JSON dau ra hop le: khong co `NaN`, timestamp duoc chuan hoa ISO-8601.

## Role 3 - Observability & QA

- Kiem tra do dai noi dung toi thieu.
- Loai noi dung loi/doc hai: null pointer, stack trace, SQL injection, script tag.
- CSV: bat thieu truong bat buoc, gia/ton kho khong hop le, gia am chua duoc sua.
- HTML: bat thieu ma san pham/ten san pham, ton kho am.
- PDF: bat noi dung qua ngan, ghi metadata file.
- Transcript/Video: bat timestamp/noise token con sot lai, chuan hoa `detected_price_vnd`.
- LegacyCode: danh dau discrepancy giua comment va logic thue VAT.

## Role 4 - DevOps & Integration

- DAG xu ly 5 nguon theo thu tu: PDF -> Transcript -> HTML -> CSV -> LegacyCode.
- Ket qua chi ghi sau khi qua quality gate.
- Ghi thong ke SLA theo tung stage vao `pipeline_report.json`.
- Dam bao output JSON dung chuan voi `allow_nan=False`.
- Tuong thich forensic: `source_type` transcript duoc map thanh `Video`, CSV co `document_id` dang `csv-{record_id}`.
- PDF uu tien Gemini API; khi thieu `GEMINI_API_KEY`, fallback sang `pypdf` de pipeline local van hoan tat.

## Forensic Debrief

- No duplicate IDs in CSV processing: PASS.
- Correct price extracted from Vietnamese audio transcript: PASS.
- Quality gate rejected corrupt content: PASS.
- Final Forensic Score: 3/3.
