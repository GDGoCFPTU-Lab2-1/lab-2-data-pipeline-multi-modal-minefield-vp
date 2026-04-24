from bs4 import BeautifulSoup
from datetime import datetime
from schema import UnifiedDocument
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract product data from the HTML table, ignoring boilerplate.

def parse_html_catalog(file_path):
    """
    Parse HTML product catalog, extracting data from the main table.
    Handles N/A and "Liên hệ" (contact needed) values gracefully.
    
    Args:
        file_path: Path to the HTML file
        
    Returns:
        List of UnifiedDocument instances (one per product)
    """
    # --- FILE READING ---
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'html.parser')
    except Exception as e:
        print(f"Error reading HTML file: {str(e)}")
        return []
    
    documents = []
    
    # Find the main product table by ID
    main_table = soup.find('table', {'id': 'main-catalog'})
    
    if not main_table:
        print("Error: Could not find main catalog table (id='main-catalog')")
        return []
    
    # Extract table headers
    headers = []
    thead = main_table.find('thead')
    if thead:
        header_cells = thead.find_all('th')
        headers = [th.get_text(strip=True) for th in header_cells]
    
    print(f"Found table headers: {headers}")
    
    # Extract table rows
    tbody = main_table.find('tbody')
    if not tbody:
        print("Error: Could not find tbody in main catalog table")
        return []
    
    rows = tbody.find_all('tr')
    
    for row_idx, row in enumerate(rows):
        try:
            cells = row.find_all('td')
            
            if len(cells) == 0:
                continue
            
            # Map cells to headers
            row_data = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    row_data[headers[i]] = cell.get_text(strip=True)
                else:
                    row_data[f'column_{i}'] = cell.get_text(strip=True)
            
            # Extract key fields
            product_id = row_data.get('Mã SP', f'product_{row_idx}')
            product_name = row_data.get('Tên sản phẩm', 'Unknown')
            category = row_data.get('Danh mục', 'Unknown')
            price_text = row_data.get('Giá niêm yết', 'N/A')
            stock = row_data.get('Tồn kho', '0')
            rating = row_data.get('Đánh giá', 'N/A')
            
            # Normalize price
            price_value = _normalize_price(price_text)
            
            # Normalize stock (handle negative values)
            try:
                stock_int = int(stock) if stock != '' else 0
                stock_int = max(0, stock_int)  # Ensure non-negative
            except (ValueError, TypeError):
                stock_int = 0
            
            # Create content summary
            content = f"Product: {product_name}\n"
            content += f"Category: {category}\n"
            content += f"Product ID: {product_id}\n"
            
            if price_value is not None:
                content += f"Price: {price_value} VND\n"
            else:
                content += f"Price: {price_text} (not available)\n"
            
            content += f"Stock: {stock_int} units\n"
            content += f"Rating: {rating}"
            
            # Create UnifiedDocument for this product
            doc = UnifiedDocument(
                source_type="HTML",
                content=content,
                author="VinShop",
                title=f"Product Listing: {product_name}",
                timestamp=datetime.now(),
                tags=["html", "ecommerce", "product", category.lower()],
                source_metadata={
                    "product_id": product_id,
                    "product_name": product_name,
                    "category": category,
                    "price_normalized": price_value,
                    "price_original": price_text,
                    "stock_quantity": stock_int,
                    "rating": rating,
                    "all_fields": row_data
                },
                processing_metadata={
                    "source": "html_catalog",
                    "extraction_method": "beautifulsoup",
                    "row_index": row_idx
                }
            )
            
            documents.append(doc)
            print(f"✓ Extracted product: {product_name} (ID: {product_id})")
            
        except Exception as e:
            print(f"Warning: Error processing row {row_idx}: {str(e)}")
            continue
    
    print(f"✓ Extracted {len(documents)} products from HTML catalog")
    return documents


def _normalize_price(price_text: str) -> float:
    """
    Normalize price text to float value in VND.
    Handles: "28,500,000 VND", "N/A", "Liên hệ", etc.
    """
    if not price_text or price_text.strip() in ['N/A', 'Liên hệ', 'Contact', '']:
        return None
    
    # Remove currency text
    cleaned = price_text.replace('VND', '').replace('USD', '').strip()
    
    # Remove commas (Vietnamese separator)
    cleaned = cleaned.replace(',', '')
    
    # Extract numbers
    try:
        # Find all numeric parts
        numbers = re.findall(r'\d+', cleaned)
        if numbers:
            # Join them (handles cases like "28500000")
            price = float(numbers[-1])  # Take the last number found
            return price
    except (ValueError, TypeError):
        pass
    
    return None

