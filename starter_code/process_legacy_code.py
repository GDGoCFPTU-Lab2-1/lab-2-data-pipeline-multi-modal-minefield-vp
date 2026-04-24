import ast
import re
from datetime import datetime
from schema import UnifiedDocument

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract docstrings and comments from legacy Python code.

def extract_logic_from_code(file_path):
    """
    Extract business logic from legacy Python code.
    Extracts: module docstring, function docstrings, and business rules from comments.
    
    Args:
        file_path: Path to the Python source file
        
    Returns:
        UnifiedDocument instance or None if extraction fails
    """
    # --- FILE READING ---
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            source_code = f.read()
    except Exception as e:
        print(f"Error reading source code file: {str(e)}")
        return None
    
    try:
        # Parse the AST
        tree = ast.parse(source_code)
    except SyntaxError as e:
        print(f"Error parsing Python file: {str(e)}")
        return None
    
    # Extract information
    module_docstring = ast.get_docstring(tree) or "No module docstring"
    functions = _extract_function_docstrings(tree)
    business_rules = _extract_business_rules(source_code)
    warnings = _extract_warnings(source_code)
    metadata = _extract_metadata(source_code)
    
    # Build content
    content = "=== MODULE DOCUMENTATION ===\n"
    content += module_docstring + "\n\n"
    
    if functions:
        content += "=== FUNCTIONS & BUSINESS LOGIC ===\n"
        for func_name, docstring in functions:
            content += f"\n### {func_name}()\n"
            content += docstring + "\n"
    
    if business_rules:
        content += "\n=== EXTRACTED BUSINESS RULES ===\n"
        for rule in business_rules:
            content += f"• {rule}\n"
    
    if warnings:
        content += "\n=== WARNINGS & NOTES ===\n"
        for warning in warnings:
            content += f"⚠️  {warning}\n"
    
    # Create document
    doc = UnifiedDocument(
        source_type="LegacyCode",
        content=content,
        author=metadata.get('author', 'Unknown'),
        title=metadata.get('title', 'Legacy Code Documentation'),
        timestamp=datetime.now(),
        tags=["legacy-code", "python", "business-logic", "documentation"],
        source_metadata={
            "module_docstring": module_docstring,
            "functions": functions,
            "business_rules": business_rules,
            "warnings": warnings,
            "metadata": metadata,
            "total_functions": len(functions),
            "total_rules": len(business_rules),
            "total_warnings": len(warnings)
        },
        processing_metadata={
            "extraction_method": "ast_parsing",
            "source": "legacy_code",
            "version_extracted": metadata.get('version', 'unknown')
        }
    )
    
    print(f"✓ Extracted {len(functions)} functions and {len(business_rules)} business rules from legacy code")
    return doc


def _extract_function_docstrings(tree: ast.AST) -> list:
    """
    Extract all function docstrings from the AST.
    
    Returns:
        List of tuples: (function_name, docstring)
    """
    functions = []
    
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            docstring = ast.get_docstring(node) or "(No docstring)"
            functions.append((node.name, docstring))
    
    return functions


def _extract_business_rules(source_code: str) -> list:
    """
    Extract business rules from comments containing "Business Logic Rule".
    
    Returns:
        List of business rule strings
    """
    rules = []
    
    # Find lines containing business logic rules
    pattern = r'(?:#+.*Business Logic Rule.*|#.*:)'
    matches = re.finditer(pattern, source_code, re.IGNORECASE | re.MULTILINE)
    
    for match in matches:
        # Get the line and some context
        line_start = source_code.rfind('\n', 0, match.start()) + 1
        line_end = source_code.find('\n', match.end())
        if line_end == -1:
            line_end = len(source_code)
        
        line = source_code[line_start:line_end].strip()
        if line and line.startswith('#'):
            rule_text = line.lstrip('#').strip()
            if rule_text:
                rules.append(rule_text)
    
    return rules


def _extract_warnings(source_code: str) -> list:
    """
    Extract warnings and important notes from comments.
    
    Returns:
        List of warning strings
    """
    warnings = []
    
    # Find WARNING, IMPORTANT, NOTE patterns
    pattern = r'#.*(?:WARNING|IMPORTANT|NOTE|DEPRECATED|DISCREPANCY|BUG|TODO).*'
    matches = re.finditer(pattern, source_code, re.IGNORECASE)
    
    for match in matches:
        warning_text = match.group(0).lstrip('#').strip()
        if warning_text:
            warnings.append(warning_text)
    
    return warnings


def _extract_metadata(source_code: str) -> dict:
    """
    Extract metadata from module docstring and comments.
    
    Returns:
        Dictionary with metadata keys like: author, version, purpose, etc.
    """
    metadata = {
        'author': 'Unknown',
        'version': 'Unknown',
        'purpose': 'Unknown',
        'title': 'Legacy Code Documentation'
    }
    
    lines = source_code.split('\n')[:30]  # Check first 30 lines
    
    for line in lines:
        # Extract Author
        if 'author' in line.lower():
            match = re.search(r'Author:\s*(.+)', line, re.IGNORECASE)
            if match:
                metadata['author'] = match.group(1).strip()
        
        # Extract Version
        if 'version' in line.lower():
            match = re.search(r'[Vv](?:ersion)?:?\s*(.+)', line)
            if match:
                metadata['version'] = match.group(1).strip()
        
        # Extract Purpose
        if 'purpose' in line.lower():
            match = re.search(r'Purpose:\s*(.+)', line, re.IGNORECASE)
            if match:
                metadata['purpose'] = match.group(1).strip()
    
    return metadata

