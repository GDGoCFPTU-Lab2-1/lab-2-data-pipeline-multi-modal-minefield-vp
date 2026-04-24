import json
import time
import os
from datetime import datetime

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "raw_data")


# Import role-specific modules
from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================
# Task: Orchestrate the ingestion pipeline and handle errors/SLA.

def main():
    start_time = time.time()
    final_kb = []
    pipeline_stats = {
        'total_sources': 5,
        'processed': 0,
        'passed_quality': 0,
        'failed_quality': 0,
        'errors': []
    }
    
    # --- FILE PATH SETUP ---
    pdf_path = os.path.join(RAW_DATA_DIR, "lecture_notes.pdf")
    trans_path = os.path.join(RAW_DATA_DIR, "demo_transcript.txt")
    html_path = os.path.join(RAW_DATA_DIR, "product_catalog.html")
    csv_path = os.path.join(RAW_DATA_DIR, "sales_records.csv")
    code_path = os.path.join(RAW_DATA_DIR, "legacy_pipeline.py")
    
    output_path = os.path.join(os.path.dirname(SCRIPT_DIR), "processed_knowledge_base.json")
    # ------------------------------------------
    
    print("=" * 70)
    print("🚀 STARTING MULTI-MODAL DATA PIPELINE ORCHESTRATION")
    print("=" * 70)
    print(f"Start time: {datetime.now().isoformat()}")
    print()
    
    # ========== PROCESSING PIPELINE ==========
    
    # 1. Process PDF
    print("📄 [1/5] Processing PDF...")
    try:
        doc = extract_pdf_data(pdf_path)
        if doc:
            pipeline_stats['processed'] += 1
            if run_quality_gate(doc.model_dump()):
                final_kb.append(doc)
                pipeline_stats['passed_quality'] += 1
            else:
                pipeline_stats['failed_quality'] += 1
        else:
            pipeline_stats['errors'].append("PDF extraction returned None")
    except Exception as e:
        pipeline_stats['errors'].append(f"PDF processing error: {str(e)}")
        print(f"  ✗ Error: {str(e)}")
    
    print()
    
    # 2. Process Transcript
    print("🎤 [2/5] Processing Transcript...")
    try:
        doc = clean_transcript(trans_path)
        if doc:
            pipeline_stats['processed'] += 1
            if run_quality_gate(doc.model_dump()):
                final_kb.append(doc)
                pipeline_stats['passed_quality'] += 1
            else:
                pipeline_stats['failed_quality'] += 1
        else:
            pipeline_stats['errors'].append("Transcript cleaning returned None")
    except Exception as e:
        pipeline_stats['errors'].append(f"Transcript processing error: {str(e)}")
        print(f"  ✗ Error: {str(e)}")
    
    print()
    
    # 3. Process HTML
    print("🌐 [3/5] Processing HTML Catalog...")
    try:
        docs = parse_html_catalog(html_path)
        if docs:
            for doc in docs:
                pipeline_stats['processed'] += 1
                if run_quality_gate(doc.model_dump()):
                    final_kb.append(doc)
                    pipeline_stats['passed_quality'] += 1
                else:
                    pipeline_stats['failed_quality'] += 1
        else:
            pipeline_stats['errors'].append("HTML parsing returned empty list")
    except Exception as e:
        pipeline_stats['errors'].append(f"HTML processing error: {str(e)}")
        print(f"  ✗ Error: {str(e)}")
    
    print()
    
    # 4. Process CSV
    print("📊 [4/5] Processing CSV Sales Records...")
    try:
        docs = process_sales_csv(csv_path)
        if docs:
            for doc in docs:
                pipeline_stats['processed'] += 1
                if run_quality_gate(doc.model_dump()):
                    final_kb.append(doc)
                    pipeline_stats['passed_quality'] += 1
                else:
                    pipeline_stats['failed_quality'] += 1
        else:
            pipeline_stats['errors'].append("CSV processing returned empty list")
    except Exception as e:
        pipeline_stats['errors'].append(f"CSV processing error: {str(e)}")
        print(f"  ✗ Error: {str(e)}")
    
    print()
    
    # 5. Process Legacy Code
    print("🔧 [5/5] Processing Legacy Code...")
    try:
        doc = extract_logic_from_code(code_path)
        if doc:
            pipeline_stats['processed'] += 1
            if run_quality_gate(doc.model_dump()):
                final_kb.append(doc)
                pipeline_stats['passed_quality'] += 1
            else:
                pipeline_stats['failed_quality'] += 1
        else:
            pipeline_stats['errors'].append("Legacy code extraction returned None")
    except Exception as e:
        pipeline_stats['errors'].append(f"Legacy code processing error: {str(e)}")
        print(f"  ✗ Error: {str(e)}")
    
    print()
    print("=" * 70)
    
    # ========== SAVE RESULTS ==========
    
    # Convert documents to JSON-serializable format
    kb_for_json = []
    for doc in final_kb:
        doc_dict = doc.model_dump()
        # Convert datetime to ISO format string
        if doc_dict.get('timestamp'):
            doc_dict['timestamp'] = doc_dict['timestamp'].isoformat()
        kb_for_json.append(doc_dict)
    
    # Save to file
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(kb_for_json, f, indent=2, ensure_ascii=False)
        print(f"✓ Knowledge base saved to: {output_path}")
    except Exception as e:
        print(f"✗ Error saving output: {str(e)}")
        pipeline_stats['errors'].append(f"Output save error: {str(e)}")
    
    # ========== STATISTICS ==========
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    print()
    print("📊 PIPELINE STATISTICS")
    print("-" * 70)
    print(f"Total execution time: {elapsed:.2f} seconds")
    print(f"Documents processed: {pipeline_stats['processed']}")
    print(f"Documents passed quality gate: {pipeline_stats['passed_quality']}")
    print(f"Documents failed quality gate: {pipeline_stats['failed_quality']}")
    print(f"Total valid documents in knowledge base: {len(final_kb)}")
    print()
    
    if pipeline_stats['errors']:
        print("⚠️  ERRORS ENCOUNTERED:")
        for error in pipeline_stats['errors']:
            print(f"  - {error}")
        print()
    
    print("=" * 70)
    print(f"✓ PIPELINE COMPLETED AT {datetime.now().isoformat()}")
    print("=" * 70)
    
    return final_kb, pipeline_stats


if __name__ == "__main__":
    main()
