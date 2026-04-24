import pandas as pd
import re
from datetime import datetime
from schema import UnifiedDocument

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Process sales records, handling type traps and duplicates.

def process_sales_csv(file_path):
    """
    Process CSV file with comprehensive data cleaning.
    Handles: duplicates, mixed price formats, date normalization.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        List of UnifiedDocument instances (one per unique sales record)
    """
    # --- FILE READING ---
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading CSV file: {str(e)}")
        return []
    
    print(f"Loaded CSV with {len(df)} rows")
    
    # --- STEP 1: Remove duplicates based on 'id' ---
    original_count = len(df)
    df = df.drop_duplicates(subset=['id'], keep='first')
    removed_duplicates = original_count - len(df)
    print(f"✓ Removed {removed_duplicates} duplicate records")
    
    # --- STEP 2: Clean and normalize 'price' column ---
    df['price_normalized'] = df['price'].apply(_normalize_price)
    
    # Filter out rows where price couldn't be normalized (optional - for quality control)
    # For now, we'll keep them but flag them
    
    # --- STEP 3: Normalize 'date_of_sale' column ---
    df['date_normalized'] = df['date_of_sale'].apply(_normalize_date)
    
    # --- STEP 4: Clean stock quantity (handle empty values) ---
    df['stock_quantity_clean'] = df['stock_quantity'].fillna(0).astype(int)
    
    # Remove rows with negative stock
    df = df[df['stock_quantity_clean'] >= 0]
    
    documents = []
    
    # --- Create UnifiedDocument for each sales record ---
    for idx, row in df.iterrows():
        try:
            product_id = str(row['id'])
            product_name = str(row['product_name'])
            category = str(row['category'])
            price_normalized = row['price_normalized']
            price_original = str(row['price'])
            currency = str(row['currency'])
            date_normalized = row['date_normalized']
            seller_id = str(row['seller_id'])
            stock = row['stock_quantity_clean']
            
            # Create content
            content = f"Sales Record - {product_name}\n"
            content += f"Category: {category}\n"
            content += f"Price: {price_original} ({currency})"
            
            if price_normalized is not None:
                content += f" → Normalized: {price_normalized}\n"
            else:
                content += " → Could not normalize price\n"
            
            content += f"Sale Date: {date_normalized}\n"
            content += f"Seller: {seller_id}\n"
            content += f"Stock: {stock} units"
            
            # Create document
            doc = UnifiedDocument(
                source_type="CSV",
                content=content,
                author=seller_id,
                title=f"Sales: {product_name}",
                timestamp=date_normalized,
                tags=["csv", "sales", "ecommerce", category.lower()],
                source_metadata={
                    "record_id": product_id,
                    "product_name": product_name,
                    "category": category,
                    "price_original": price_original,
                    "price_normalized": price_normalized,
                    "currency": currency,
                    "date_of_sale": date_normalized.isoformat() if date_normalized else None,
                    "seller_id": seller_id,
                    "stock_quantity": stock
                },
                processing_metadata={
                    "source": "csv_sales",
                    "row_index": int(idx),
                    "price_extraction_status": "success" if price_normalized is not None else "warning",
                    "date_extraction_status": "success" if date_normalized is not None else "warning"
                }
            )
            
            documents.append(doc)
            
        except Exception as e:
            print(f"Warning: Error processing CSV row {idx}: {str(e)}")
            continue
    
    print(f"✓ Processed {len(documents)} valid sales records from CSV")
    return documents


def _normalize_price(price_str) -> float:
    """
    Normalize price to float value.
    Handles: "$1200", "250000", "five dollars", "N/A", "NULL", etc.
    """
    if pd.isna(price_str) or price_str in ['N/A', 'NULL', 'null', '', 'Liên hệ']:
        return None
    
    price_str = str(price_str).strip()
    
    # Handle word-based prices
    if 'five dollars' in price_str.lower():
        return 5.0
    if 'dollar' in price_str.lower() or 'usd' in price_str.lower():
        numbers = re.findall(r'\d+\.?\d*', price_str)
        if numbers:
            return float(numbers[0])
    
    # Remove currency symbols and spaces
    cleaned = re.sub(r'[$,\s]', '', price_str)
    
    # Extract numbers
    try:
        match = re.search(r'(\d+\.?\d*)', cleaned)
        if match:
            return float(match.group(1))
    except (ValueError, AttributeError):
        pass
    
    return None


def _normalize_date(date_str) -> datetime:
    """
    Normalize date string to datetime object.
    Handles: "2026-01-15", "15/01/2026", "January 16th 2026", "19 Jan 2026", etc.
    """
    if pd.isna(date_str) or date_str in ['', 'N/A', 'NULL']:
        return None
    
    date_str = str(date_str).strip()
    
    # List of date formats to try (most specific to least specific)
    date_formats = [
        '%Y-%m-%d',           # 2026-01-15
        '%d/%m/%Y',           # 15/01/2026
        '%d-%m-%Y',           # 15-01-2026
        '%d %b %Y',           # 19 Jan 2026
        '%B %d %Y',           # January 15 2026
        '%B %dth %Y',         # January 15th 2026
        '%Y/%m/%d',           # 2026/01/15
    ]
    
    for date_format in date_formats:
        try:
            # Clean up ordinal suffixes (st, nd, rd, th)
            cleaned = re.sub(r'(\d)(st|nd|rd|th)', r'\1', date_str)
            return pd.to_datetime(cleaned, format=date_format)
        except (ValueError, TypeError):
            continue
    
    # Try pandas default parser as last resort
    try:
        return pd.to_datetime(date_str)
    except (ValueError, TypeError):
        return None

