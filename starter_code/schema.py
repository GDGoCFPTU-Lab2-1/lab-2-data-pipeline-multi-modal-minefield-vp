from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid

# ==========================================
# ROLE 1: LEAD DATA ARCHITECT
# ==========================================
# UNIFIED SCHEMA v1 - Foundation for all multi-modal data sources
# This schema is designed to accommodate PDF, Transcript, HTML, CSV, and Legacy Code
# IMPORTANT: A breaking change (v2) is coming at 11:00 AM - be prepared!

class UnifiedDocument(BaseModel):
    """
    Unified document schema for all data sources.
    
    Fields:
    - document_id: Unique identifier (auto-generated UUID if not provided)
    - source_type: Type of source ('PDF', 'Transcript', 'HTML', 'CSV', 'LegacyCode')
    - content: Main textual content or extracted data
    - author: Creator/owner of the document (if available)
    - timestamp: When the document was created/modified (if available)
    - title: Human-readable title/description
    - metadata: Flexible dict for source-specific attributes
    - tags: List of classification tags for searchability
    """
    
    document_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique document identifier")
    source_type: str = Field(description="Source type: PDF, Transcript, HTML, CSV, or LegacyCode")
    content: str = Field(description="Main extracted content")
    author: Optional[str] = Field(default="Unknown", description="Document author/creator")
    title: Optional[str] = Field(default=None, description="Document title or description")
    timestamp: Optional[datetime] = Field(default=None, description="Creation/modification timestamp")
    tags: List[str] = Field(default_factory=list, description="Classification tags")
    
    # Source-specific metadata (flexible dict to handle any custom fields)
    source_metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Source-specific attributes (e.g., PDF pages, speaker names, product IDs)"
    )
    
    # Quality/processing metadata
    processing_metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Pipeline processing info (e.g., extraction_method, confidence_score)"
    )
    
    @validator('source_type')
    def validate_source_type(cls, v):
        """Ensure source_type is one of the accepted types."""
        valid_types = {'PDF', 'Transcript', 'HTML', 'CSV', 'LegacyCode'}
        if v not in valid_types:
            raise ValueError(f"source_type must be one of {valid_types}")
        return v
    
    @validator('content')
    def validate_content_not_empty(cls, v):
        """Ensure content has meaningful length."""
        if not v or len(v.strip()) < 5:
            raise ValueError("content must have at least 5 characters")
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "document_id": "550e8400-e29b-41d4-a716-446655440000",
                "source_type": "PDF",
                "content": "Sample extracted PDF content...",
                "author": "John Doe",
                "title": "Lecture Notes on Data Pipelines",
                "timestamp": "2026-01-15T10:30:00",
                "tags": ["technical", "data-engineering"],
                "source_metadata": {"page_count": 25, "language": "en"},
                "processing_metadata": {"extraction_method": "gemini-api", "version": "v1"}
            }
        }
