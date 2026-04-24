import re
from datetime import datetime
from schema import UnifiedDocument

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Clean the transcript text and extract key information.

NOISE_PATTERNS = [
    r'\[Music[^\]]*\]',
    r'\[inaudible[^\]]*\]',
    r'\[Laughter[^\]]*\]',
    r'\[Background[^\]]*\]',
    r'\[Silence[^\]]*\]',
    r'\[Applause[^\]]*\]',
    r'\[Speaker \d+[^\]]*\]:',  # Remove speaker labels
]

TIMESTAMP_PATTERN = r'\[\d{2}:\d{2}:\d{2}\]'

# Vietnamese number words mapping
VIETNAMESE_NUMBERS = {
    'năm trăm nghìn': 500000,
    'một triệu': 1000000,
    'hai triệu': 2000000,
    'ba triệu': 3000000,
    'bốn triệu': 4000000,
    'năm triệu': 5000000,
    'sáu triệu': 6000000,
    'bảy triệu': 7000000,
    'tám triệu': 8000000,
    'chín triệu': 9000000,
    'mười triệu': 10000000,
}


def clean_transcript(file_path):
    """
    Clean transcript by removing noise tokens and extracting structured data.
    
    Args:
        file_path: Path to the transcript text file
        
    Returns:
        UnifiedDocument instance or None if processing fails
    """
    # --- FILE READING ---
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
    except Exception as e:
        print(f"Error reading transcript file: {str(e)}")
        return None
    
    # --- CLEANING PIPELINE ---
    
    # Step 1: Remove timestamps
    cleaned_text = re.sub(TIMESTAMP_PATTERN, '', text)
    
    # Step 2: Remove noise patterns
    for pattern in NOISE_PATTERNS:
        cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE)
    
    # Step 3: Extract Vietnamese prices
    extracted_prices = _extract_vietnamese_prices(text)
    
    # Step 4: Clean up whitespace
    cleaned_text = re.sub(r'\n\s*\n', '\n', cleaned_text)  # Remove multiple blank lines
    cleaned_text = re.sub(r' +', ' ', cleaned_text)  # Remove multiple spaces
    cleaned_text = cleaned_text.strip()
    
    # Extract speakers and main topics
    speakers = _extract_speakers(text)
    topics = _extract_topics(cleaned_text)
    
    # Create UnifiedDocument
    doc = UnifiedDocument(
        source_type="Transcript",
        content=cleaned_text,
        author="; ".join(speakers) if speakers else "Unknown",
        title="Transcript: Data Pipeline Engineering Lecture",
        timestamp=datetime.now(),
        tags=["transcript", "audio", "cleaned", "vietnamese"],
        source_metadata={
            "speakers": speakers,
            "topics": topics,
            "extracted_prices_vnd": extracted_prices,
            "original_length": len(text),
            "cleaned_length": len(cleaned_text),
        },
        processing_metadata={
            "noise_removal": "applied",
            "timestamp_removal": "applied",
            "vietnamese_price_extraction": "applied"
        }
    )
    
    print(f"✓ Transcript cleaned successfully. Extracted {len(extracted_prices)} prices: {extracted_prices}")
    return doc


def _extract_vietnamese_prices(text: str) -> dict:
    """Extract Vietnamese price mentions from transcript."""
    prices = {}
    
    for vn_word, value in VIETNAMESE_NUMBERS.items():
        if vn_word.lower() in text.lower():
            prices[vn_word] = value
    
    return prices


def _extract_speakers(text: str) -> list:
    """Extract unique speaker identifiers from transcript."""
    speaker_pattern = r'\[Speaker (\d+)\]'
    speakers = re.findall(speaker_pattern, text)
    return sorted(list(set(speakers)))


def _extract_topics(text: str) -> list:
    """Extract key topics mentioned in transcript."""
    # Keywords to look for
    keywords = [
        'Data Pipeline', 'Semantic Drift', 'Zillow', 
        'Machine Learning', 'Data Quality', 'Pricing'
    ]
    
    found_topics = []
    for keyword in keywords:
        if keyword.lower() in text.lower():
            found_topics.append(keyword)
    
    return found_topics

