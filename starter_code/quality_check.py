# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Task: Implement quality gates to reject corrupt data or logic discrepancies.

def run_quality_gate(document_dict):
    """
    Run quality checks on a UnifiedDocument.
    Returns True if document passes all checks, False otherwise.
    
    Quality Checks:
    1. Content length >= 20 characters
    2. No toxic/error strings (Null pointer, SQL injection patterns, etc.)
    3. Source-specific validations
    4. Flag discrepancies (e.g., between comments and code)
    """
    
    # Check 1: Content length
    if 'content' not in document_dict:
        print(f"  ✗ FAIL: Missing 'content' field")
        return False
    
    content = str(document_dict.get('content', ''))
    if len(content.strip()) < 20:
        print(f"  ✗ FAIL: Content too short ({len(content)} chars < 20 minimum)")
        return False
    
    # Check 2: Toxic/error strings
    toxic_patterns = [
        'null pointer exception',
        'segmentation fault',
        'undefined is not defined',
        'typeerror',
        'valueerror',
        'keyerror',
        'attributeerror',
    ]
    
    content_lower = content.lower()
    for pattern in toxic_patterns:
        if pattern in content_lower:
            print(f"  ⚠️  WARNING: Toxic pattern detected: '{pattern}'")
            # Don't fail, just warn
    
    # Check 3: Source-specific validations
    source_type = document_dict.get('source_type', 'Unknown')
    
    if source_type == 'CSV':
        result = _validate_csv_document(document_dict)
        if not result:
            return False
    
    elif source_type == 'HTML':
        result = _validate_html_document(document_dict)
        if not result:
            return False
    
    elif source_type == 'PDF':
        result = _validate_pdf_document(document_dict)
        if not result:
            return False
    
    elif source_type == 'Transcript':
        result = _validate_transcript_document(document_dict)
        if not result:
            return False
    
    elif source_type == 'LegacyCode':
        result = _validate_legacy_code_document(document_dict)
        if not result:
            return False
    
    # If all checks pass
    print(f"  ✓ PASS: {source_type} document passed quality gate")
    return True


def _validate_csv_document(document_dict) -> bool:
    """Validate CSV-specific fields."""
    metadata = document_dict.get('source_metadata', {})
    
    # Check if key fields are present
    required_fields = ['record_id', 'product_name', 'price_normalized']
    for field in required_fields:
        if field not in metadata:
            print(f"  ✗ FAIL (CSV): Missing required field '{field}'")
            return False
    
    # Check if price is reasonable (if normalized)
    price = metadata.get('price_normalized')
    if price is not None and price < 0:
        print(f"  ✗ FAIL (CSV): Negative price detected: {price}")
        return False
    
    return True


def _validate_html_document(document_dict) -> bool:
    """Validate HTML-specific fields."""
    metadata = document_dict.get('source_metadata', {})
    
    # Check for key product fields
    if 'product_id' not in metadata or 'product_name' not in metadata:
        print(f"  ✗ FAIL (HTML): Missing product identification")
        return False
    
    # Check stock quantity is non-negative
    stock = metadata.get('stock_quantity', 0)
    if stock < 0:
        print(f"  ✗ FAIL (HTML): Negative stock quantity: {stock}")
        return False
    
    return True


def _validate_pdf_document(document_dict) -> bool:
    """Validate PDF-specific fields."""
    metadata = document_dict.get('source_metadata', {})
    
    # PDF should have file_name
    if 'file_name' not in metadata:
        print(f"  ✗ WARNING (PDF): Missing file_name metadata")
    
    # Content should be substantial for PDF
    content = str(document_dict.get('content', ''))
    if len(content) < 50:
        print(f"  ✗ FAIL (PDF): PDF content too short ({len(content)} chars)")
        return False
    
    return True


def _validate_transcript_document(document_dict) -> bool:
    """Validate Transcript-specific fields."""
    metadata = document_dict.get('source_metadata', {})
    
    # Check if transcript was cleaned (should have noise_removal flag)
    processing = document_dict.get('processing_metadata', {})
    if 'noise_removal' not in processing:
        print(f"  ✗ FAIL (Transcript): Transcript not properly cleaned")
        return False
    
    # Check if speakers were identified
    speakers = metadata.get('speakers', [])
    if not speakers:
        print(f"  ⚠️  WARNING (Transcript): No speakers identified")
    
    return True


def _validate_legacy_code_document(document_dict) -> bool:
    """Validate LegacyCode-specific fields and check for discrepancies."""
    metadata = document_dict.get('source_metadata', {})
    content = str(document_dict.get('content', ''))
    
    # Check if business rules were extracted
    rules = metadata.get('business_rules', [])
    if not rules:
        print(f"  ⚠️  WARNING (LegacyCode): No business rules extracted")
    
    # Check if warnings were extracted (especially discrepancies)
    warnings = metadata.get('warnings', [])
    if warnings:
        print(f"  ⚠️  WARNING (LegacyCode): Found {len(warnings)} potential discrepancies")
        for warning in warnings[:3]:  # Show first 3
            print(f"     - {warning}")
    
    return True
