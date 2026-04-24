import google.generativeai as genai
import os
import time
from pathlib import Path
from schema import UnifiedDocument
from datetime import datetime

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Use Gemini API to extract structured data from lecture_notes.pdf
# SLA: Implement exponential backoff for 429 errors

def extract_pdf_data(file_path):
    """
    Extract PDF content using Gemini API with exponential backoff.
    
    Args:
        file_path: Path to the PDF file
        
    Returns:
        UnifiedDocument instance or None if extraction fails
    """
    # --- FILE CHECK ---
    if not os.path.exists(file_path):
        print(f"Error: PDF file not found at {file_path}")
        return None
    
    try:
        # Initialize Gemini API - read from environment variable
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            print("Error: GEMINI_API_KEY environment variable not set")
            return None
        
        genai.configure(api_key=api_key)
        
        # Read PDF file
        with open(file_path, 'rb') as f:
            pdf_content = f.read()
        
        # Prepare the file for Gemini
        file_name = Path(file_path).name
        
        # Call Gemini API with exponential backoff for rate limiting (429 errors)
        max_retries = 3
        retry_delay = 1  # Start with 1 second
        
        for attempt in range(max_retries):
            try:
                # Upload file to Gemini
                files = [
                    {
                        'mime_type': 'application/pdf',
                        'data': pdf_content
                    }
                ]
                
                # Create extraction prompt
                prompt = """Please extract the following from this PDF:
                1. Title of the document
                2. Author (if mentioned)
                3. Main topics covered (as a comma-separated list)
                4. Any tables or structured data found
                5. Key statistics or figures mentioned
                
                Format your response as a structured text with clear sections."""
                
                # Use Gemini API
                model = genai.GenerativeModel('gemini-2.0-flash-exp')
                
                # Note: For production, you'd upload files, but for basic text extraction:
                response = model.generate_content([prompt, files[0] if files else ""])
                
                extracted_text = response.text
                
                # Parse the response
                doc = UnifiedDocument(
                    source_type="PDF",
                    content=extracted_text,
                    author=_extract_author_from_response(extracted_text),
                    title=_extract_title_from_response(extracted_text),
                    timestamp=datetime.now(),
                    tags=["pdf", "extracted", "gemini-api"],
                    source_metadata={
                        "file_name": file_name,
                        "file_size_bytes": len(pdf_content),
                        "extraction_method": "gemini-2.0-flash"
                    },
                    processing_metadata={
                        "api_version": "2.0",
                        "model": "gemini-2.0-flash-exp",
                        "attempt": attempt + 1
                    }
                )
                
                print(f"✓ PDF extracted successfully from {file_name}")
                return doc
                
            except Exception as e:
                if "429" in str(e) or "rate_limit" in str(e).lower():
                    if attempt < max_retries - 1:
                        print(f"Rate limit hit (429). Retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        continue
                raise
        
        return None
        
    except Exception as e:
        print(f"Error extracting PDF: {str(e)}")
        return None


def _extract_title_from_response(response_text: str) -> str:
    """Extract title from Gemini response."""
    lines = response_text.split('\n')
    for line in lines:
        if 'title' in line.lower():
            return line.split(':', 1)[-1].strip() if ':' in line else "Extracted from PDF"
    return "Extracted from PDF"


def _extract_author_from_response(response_text: str) -> str:
    """Extract author from Gemini response."""
    lines = response_text.split('\n')
    for line in lines:
        if 'author' in line.lower():
            return line.split(':', 1)[-1].strip() if ':' in line else "Unknown"
    return "Unknown"

