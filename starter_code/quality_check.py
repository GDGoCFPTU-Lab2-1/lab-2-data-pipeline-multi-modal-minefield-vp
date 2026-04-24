import math
import re
from typing import Any, Dict


# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Task: Implement semantic gates to reject corrupt data and flag hidden logic issues.

MIN_CONTENT_LENGTH = 20

TOXIC_PATTERNS = [
    r"null pointer exception",
    r"segmentation fault",
    r"undefined is not defined",
    r"\b(typeerror|valueerror|keyerror|attributeerror)\b",
    r"traceback \(most recent call last\)",
    r"\b(drop|delete|truncate)\s+table\b",
    r"union\s+select",
    r"<script\b",
]


def run_quality_gate(document_dict: Dict[str, Any]) -> bool:
    """
    Return True only when the document is safe for the final knowledge base.
    The dict may be enriched with QA metadata that the orchestrator persists.
    """
    if "content" not in document_dict:
        _fail("Missing 'content' field")
        return False

    content = str(document_dict.get("content", ""))
    if len(content.strip()) < MIN_CONTENT_LENGTH:
        _fail(f"Content too short ({len(content)} chars < {MIN_CONTENT_LENGTH} minimum)")
        return False

    toxic_hits = _find_toxic_patterns(content)
    if toxic_hits:
        _fail(f"Toxic/corrupt content detected: {', '.join(toxic_hits)}")
        return False

    source_type = document_dict.get("source_type", "Unknown")
    validators = {
        "CSV": _validate_csv_document,
        "HTML": _validate_html_document,
        "PDF": _validate_pdf_document,
        "Transcript": _validate_transcript_document,
        "Video": _validate_transcript_document,
        "LegacyCode": _validate_legacy_code_document,
    }

    validator = validators.get(source_type)
    if validator and not validator(document_dict):
        return False

    _mark_quality_status(document_dict, "passed")
    print(f"  PASS: {source_type} document passed quality gate")
    return True


def _validate_csv_document(document_dict: Dict[str, Any]) -> bool:
    metadata = document_dict.get("source_metadata", {})

    required_fields = ["record_id", "product_name", "price_normalized"]
    for field in required_fields:
        if field not in metadata:
            _fail(f"CSV missing required field '{field}'")
            return False

    price = metadata.get("price_normalized")
    if not _is_valid_optional_number(price):
        _fail(f"CSV invalid price value: {price}")
        return False

    original_price = str(metadata.get("price_original", "")).strip()
    price_correction = metadata.get("price_correction_applied")
    if original_price.startswith("-") and price_correction != "negative_to_absolute":
        _fail(f"CSV negative original price detected: {original_price}")
        return False

    if price is not None and price < 0:
        _fail(f"CSV negative price detected: {price}")
        return False

    stock = metadata.get("stock_quantity")
    if not _is_valid_optional_number(stock) or (stock is not None and stock < 0):
        _fail(f"CSV invalid stock quantity: {stock}")
        return False

    return True


def _validate_html_document(document_dict: Dict[str, Any]) -> bool:
    metadata = document_dict.get("source_metadata", {})

    if "product_id" not in metadata or "product_name" not in metadata:
        _fail("HTML missing product identification")
        return False

    stock = metadata.get("stock_quantity", 0)
    if not _is_valid_optional_number(stock) or stock < 0:
        _fail(f"HTML invalid stock quantity: {stock}")
        return False

    return True


def _validate_pdf_document(document_dict: Dict[str, Any]) -> bool:
    metadata = document_dict.get("source_metadata", {})

    if "file_name" not in metadata:
        _warn("PDF missing file_name metadata")

    content = str(document_dict.get("content", ""))
    if len(content) < 50:
        _fail(f"PDF content too short ({len(content)} chars)")
        return False

    return True


def _validate_transcript_document(document_dict: Dict[str, Any]) -> bool:
    metadata = document_dict.get("source_metadata", {})
    content = str(document_dict.get("content", ""))
    processing = document_dict.get("processing_metadata", {})

    if "noise_removal" not in processing:
        _fail("Transcript not properly cleaned")
        return False

    has_timestamp = re.search(r"\[\d{2}:\d{2}:\d{2}\]", content)
    has_noise = re.search(r"\[(music|inaudible|laughter|applause)", content, re.IGNORECASE)
    if has_timestamp or has_noise:
        _fail("Transcript still contains timestamp/noise tokens")
        return False

    speakers = metadata.get("speakers", [])
    if not speakers:
        _warn("Transcript has no speakers identified")

    detected_price = metadata.get("detected_price_vnd")
    extracted_prices = metadata.get("extracted_prices_vnd", {})
    if detected_price is None and isinstance(extracted_prices, dict) and extracted_prices:
        detected_price = next(iter(extracted_prices.values()))
        metadata["detected_price_vnd"] = detected_price

    if detected_price is not None and (not _is_valid_optional_number(detected_price) or detected_price <= 0):
        _fail(f"Transcript invalid detected price: {detected_price}")
        return False

    return True


def _validate_legacy_code_document(document_dict: Dict[str, Any]) -> bool:
    metadata = document_dict.get("source_metadata", {})
    content = str(document_dict.get("content", ""))

    if not metadata.get("business_rules"):
        _warn("LegacyCode has no business rules extracted")

    discrepancies = list(metadata.get("discrepancies", []))
    warnings = metadata.get("warnings", [])

    if "8%" in content and ("0.10" in content or "10%" in content):
        discrepancies.append("legacy_tax_calc comment says 8% but code applies 10% VAT")

    if any("discrepancy" in str(item).lower() or "misleading" in str(item).lower() for item in warnings):
        discrepancies.append("Legacy code contains misleading/discrepancy warning comments")

    if discrepancies:
        unique_discrepancies = sorted(set(discrepancies))
        metadata["discrepancies"] = unique_discrepancies
        _warn(f"LegacyCode discrepancies flagged: {len(unique_discrepancies)}")

    return True


def _find_toxic_patterns(content: str) -> list:
    hits = []
    for pattern in TOXIC_PATTERNS:
        if re.search(pattern, content, flags=re.IGNORECASE):
            hits.append(pattern)
    return hits


def _is_valid_optional_number(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, bool):
        return False
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number)


def _mark_quality_status(document_dict: Dict[str, Any], status: str) -> None:
    document_dict.setdefault("processing_metadata", {})["quality_gate"] = status


def _fail(message: str) -> None:
    print(f"  FAIL: {message}")


def _warn(message: str) -> None:
    print(f"  WARN: {message}")
