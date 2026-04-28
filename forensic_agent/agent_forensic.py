import json
import os
import sys

def run_forensic_test():
    print("=== STARTING FORENSIC DEBRIEF ===")
    
    # Xác định đường dẫn file linh hoạt
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_path = os.path.join(os.path.dirname(script_dir), "processed_knowledge_base.json")
    
    # Kiểm tra sự tồn tại của file để tránh crash toàn bộ pipeline
    if not os.path.exists(base_path):
        base_path = "processed_knowledge_base.json"
        if not os.path.exists(base_path):
            print("Error: processed_knowledge_base.json not found. Pipeline failed.")
            sys.exit(1) # Trả về lỗi để Github Actions báo đỏ
            
    try:
        with open(base_path, "r", encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading JSON: {e}")
        sys.exit(1)

    score = 0
    total = 3
    
    # Đảm bảo data là list để tránh lỗi lặp
    if not isinstance(data, list):
        print("[FAIL] Data format is not a list.")
        sys.exit(1)

    # --- Q1: Check for duplicate avoidance ---
    # Lọc các ID từ CSV một cách an toàn
    csv_ids = [d.get('document_id', 'unknown') for d in data if isinstance(d, dict) and 'csv-' in str(d.get('document_id', ''))]
    
    if len(csv_ids) > 0 and len(csv_ids) == len(set(csv_ids)):
        print("[PASS] No duplicate IDs in CSV processing.")
        score += 1
    else:
        print("[FAIL] Duplicate IDs detected or no CSV data found.")
        
    # --- Q2: Check for price extraction from transcript ---
    # Hỗ trợ cả 'Transcript' hoặc 'Video' như trong code cũ của bạn
    transcript_doc = next((d for d in data if isinstance(d, dict) and d.get('source_type') in ['Transcript', 'Video']), None)
    
    if transcript_doc and transcript_doc.get('source_metadata', {}).get('detected_price_vnd') == 500000:
        print("[PASS] Correct price extracted (500,000 VND).")
        score += 1
    else:
        print("[FAIL] Failed to extract correct price from audio/video metadata.")
        
    # --- Q3: Check for quality gate effectiveness ---
    # Kiểm tra an toàn để không bị dừng script nếu content bị thiếu
    corrupt_found = any("Null pointer exception" in str(d.get('content', '')) for d in data if isinstance(d, dict))
    
    if not corrupt_found:
        print("[PASS] Quality gate successfully rejected corrupt content.")
        score += 1
    else:
        print("[FAIL] Quality gate failed: Corrupt data found.")
        
    print(f"\nFinal Forensic Score: {score}/{total}")

    # --- QUAN TRỌNG: Output cho GitHub Autograding ---
    # Nếu bạn dùng chấm điểm dựa trên Exit Code, script phải exit 0 nếu pass hết.
    # Tuy nhiên, cách tốt nhất là luôn in điểm rõ ràng.
    if score < total:
        # Bạn có thể chọn sys.exit(1) nếu muốn đánh dấu là "Trượt" trên GitHub
        # Hoặc để sys.exit(0) nếu chỉ muốn lấy điểm số từ logs
        pass 

if __name__ == "__main__":
    run_forensic_test()
