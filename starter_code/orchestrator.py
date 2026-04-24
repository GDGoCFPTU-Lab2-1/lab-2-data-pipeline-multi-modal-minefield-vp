import json
import math
import os
import sys
import time
from datetime import date, datetime
from typing import Any, Callable, Iterable, List, Tuple

from process_csv import process_sales_csv
from process_html import parse_html_catalog
from process_legacy_code import extract_logic_from_code
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from quality_check import run_quality_gate


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "raw_data")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "processed_knowledge_base.json")
REPORT_PATH = os.path.join(PROJECT_ROOT, "pipeline_report.json")


# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================
# Task: Orchestrate the ingestion DAG, enforce QA gates, and persist valid JSON.

PipelineStage = Tuple[str, str, Callable[[str], Any], bool]


def main():
    start_time = time.time()
    started_at = datetime.now()
    final_kb: List[dict] = []
    pipeline_stats = {
        "total_sources": 5,
        "processed": 0,
        "passed_quality": 0,
        "failed_quality": 0,
        "errors": [],
        "stage_timings_seconds": {},
        "started_at": started_at.isoformat(),
    }

    stages: List[PipelineStage] = [
        ("PDF", os.path.join(RAW_DATA_DIR, "lecture_notes.pdf"), extract_pdf_data, False),
        ("Transcript", os.path.join(RAW_DATA_DIR, "demo_transcript.txt"), clean_transcript, False),
        ("HTML", os.path.join(RAW_DATA_DIR, "product_catalog.html"), parse_html_catalog, True),
        ("CSV", os.path.join(RAW_DATA_DIR, "sales_records.csv"), process_sales_csv, True),
        ("LegacyCode", os.path.join(RAW_DATA_DIR, "legacy_pipeline.py"), extract_logic_from_code, False),
    ]

    print("=" * 70)
    print("STARTING MULTI-MODAL DATA PIPELINE ORCHESTRATION")
    print("=" * 70)
    print(f"Start time: {started_at.isoformat()}")

    for index, (stage_name, input_path, processor, returns_many) in enumerate(stages, start=1):
        print()
        print(f"[{index}/{len(stages)}] Processing {stage_name}...")
        stage_start = time.time()

        try:
            result = processor(input_path)
            documents = list(result) if returns_many and result else ([result] if result else [])

            if not documents:
                pipeline_stats["errors"].append(f"{stage_name} returned no documents")
                continue

            for doc in documents:
                pipeline_stats["processed"] += 1
                doc_dict = _prepare_document_for_output(doc)
                if run_quality_gate(doc_dict):
                    final_kb.append(doc_dict)
                    pipeline_stats["passed_quality"] += 1
                else:
                    pipeline_stats["failed_quality"] += 1

        except Exception as exc:
            message = f"{stage_name} processing error: {exc}"
            pipeline_stats["errors"].append(message)
            print(f"  ERROR: {message}")
        finally:
            pipeline_stats["stage_timings_seconds"][stage_name] = round(time.time() - stage_start, 3)

    elapsed = time.time() - start_time
    pipeline_stats["completed_at"] = datetime.now().isoformat()
    pipeline_stats["execution_time_seconds"] = round(elapsed, 3)
    pipeline_stats["documents_written"] = len(final_kb)

    _write_json(OUTPUT_PATH, final_kb)
    _write_json(REPORT_PATH, pipeline_stats)
    _print_summary(pipeline_stats)

    return final_kb, pipeline_stats


def _prepare_document_for_output(doc: Any) -> dict:
    if hasattr(doc, "model_dump"):
        doc_dict = doc.model_dump()
    elif isinstance(doc, dict):
        doc_dict = dict(doc)
    else:
        raise TypeError(f"Unsupported document type: {type(doc).__name__}")

    doc_dict = _sanitize_for_json(doc_dict)
    _apply_submission_compatibility(doc_dict)
    return doc_dict


def _apply_submission_compatibility(doc_dict: dict) -> None:
    source_type = doc_dict.get("source_type")
    metadata = doc_dict.setdefault("source_metadata", {})
    processing = doc_dict.setdefault("processing_metadata", {})

    if source_type == "CSV":
        record_id = str(metadata.get("record_id", doc_dict.get("document_id", "")))
        doc_dict["document_id"] = f"csv-{record_id}"

    elif source_type == "HTML":
        product_id = str(metadata.get("product_id", doc_dict.get("document_id", "")))
        doc_dict["document_id"] = f"html-{product_id}"

    elif source_type == "Transcript":
        doc_dict["source_type"] = "Video"
        processing["original_source_type"] = "Transcript"
        tags = doc_dict.setdefault("tags", [])
        if "video" not in tags:
            tags.append("video")

        extracted_prices = metadata.get("extracted_prices_vnd", {})
        if "detected_price_vnd" not in metadata and isinstance(extracted_prices, dict) and extracted_prices:
            metadata["detected_price_vnd"] = next(iter(extracted_prices.values()))

    elif source_type == "LegacyCode":
        doc_dict["document_id"] = "legacy-code-business-rules"


def _sanitize_for_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _sanitize_for_json(item) for key, item in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_sanitize_for_json(item) for item in value]

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, float):
        return value if math.isfinite(value) else None

    if hasattr(value, "isoformat") and callable(value.isoformat):
        try:
            return value.isoformat()
        except TypeError:
            pass

    return value


def _write_json(path: str, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False, allow_nan=False)
        handle.write("\n")
    print(f"Saved JSON: {path}")


def _print_summary(stats: dict) -> None:
    print()
    print("=" * 70)
    print("PIPELINE STATISTICS")
    print("-" * 70)
    print(f"Total execution time: {stats['execution_time_seconds']:.3f} seconds")
    print(f"Documents processed: {stats['processed']}")
    print(f"Documents passed quality gate: {stats['passed_quality']}")
    print(f"Documents failed quality gate: {stats['failed_quality']}")
    print(f"Documents written: {stats['documents_written']}")

    if stats["errors"]:
        print("Errors:")
        for error in stats["errors"]:
            print(f"  - {error}")

    print("=" * 70)


if __name__ == "__main__":
    main()
