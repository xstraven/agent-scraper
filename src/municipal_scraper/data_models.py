"""Data models for German municipal scraping system."""

from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, HttpUrl


class GermanState(str, Enum):
    """German federal states (Bundesländer)."""
    BADEN_WUERTTEMBERG = "Baden-Württemberg"
    BAYERN = "Bayern" 
    BERLIN = "Berlin"
    BRANDENBURG = "Brandenburg"
    BREMEN = "Bremen"
    HAMBURG = "Hamburg"
    HESSEN = "Hessen"
    MECKLENBURG_VORPOMMERN = "Mecklenburg-Vorpommern"
    NIEDERSACHSEN = "Niedersachsen"
    NORDRHEIN_WESTFALEN = "Nordrhein-Westfalen"
    RHEINLAND_PFALZ = "Rheinland-Pfalz"
    SAARLAND = "Saarland"
    SACHSEN = "Sachsen"
    SACHSEN_ANHALT = "Sachsen-Anhalt"
    SCHLESWIG_HOLSTEIN = "Schleswig-Holstein"
    THUERINGEN = "Thüringen"


class AdministrativeLevel(str, Enum):
    """Administrative levels in German system."""
    GEMEINDE = "Gemeinde"  # Municipality
    STADT = "Stadt"  # City
    AMT = "Amt"  # Administrative district
    SAMTGEMEINDE = "Samtgemeinde"  # Joint municipality
    VERBANDSGEMEINDE = "Verbandsgemeinde"  # Collective municipality
    GEMEINDEVERWALTUNGSVERBAND = "Gemeindeverwaltungsverband"  # Municipal administrative association


class RISProvider(str, Enum):
    """Known Ratsinformationssystem providers."""
    REGISAFE = "regisafe"
    SD_NET = "sd_net"
    KOMMUNE_AKTIV = "kommune_aktiv"
    ALLRIS = "allris"
    SESSIONNET = "sessionnet"
    SOMACOS = "somacos"
    CUSTOM = "custom"
    UNKNOWN = "unknown"


class MeetingType(str, Enum):
    """Types of municipal meetings."""
    GEMEINDERAT = "Gemeinderat"  # Municipal council
    STADTRAT = "Stadtrat"  # City council
    GEMEINDEVERTRETUNG = "Gemeindevertretung"  # Municipal representation
    AUSSCHUSS = "Ausschuss"  # Committee
    FINANZAUSSCHUSS = "Finanzausschuss"  # Finance committee
    BAUAUSSCHUSS = "Bauausschuss"  # Building committee
    HAUPTAUSSCHUSS = "Hauptausschuss"  # Main committee
    JUGENDAUSSCHUSS = "Jugendausschuss"  # Youth committee
    SOZIALAUSSCHUSS = "Sozialausschuss"  # Social committee
    ANDERE = "Andere"  # Other


class DocumentType(str, Enum):
    """Types of meeting documents."""
    PROTOKOLL = "Protokoll"  # Protocol/Minutes
    TAGESORDNUNG = "Tagesordnung"  # Agenda
    EINLADUNG = "Einladung"  # Invitation
    VORLAGE = "Vorlage"  # Template/Proposal
    BESCHLUSS = "Beschluss"  # Resolution
    ANLAGE = "Anlage"  # Attachment
    NIEDERSCHRIFT = "Niederschrift"  # Transcript
    ANDERE = "Andere"  # Other


class ScrapingStatus(str, Enum):
    """Status of scraping operations."""
    PENDING = "pending"
    DISCOVERED = "discovered"
    ACCESSIBLE = "accessible"
    SCRAPED = "scraped"
    FAILED = "failed"
    BLOCKED = "blocked"


class Municipality(BaseModel):
    """German municipality data model."""
    name: str
    official_name: Optional[str] = None
    state: GermanState
    administrative_level: AdministrativeLevel
    postal_code: Optional[str] = None
    municipality_key: Optional[str] = None  # AGS (Amtlicher Gemeindeschlüssel)
    population: Optional[int] = None
    area_km2: Optional[float] = None
    
    # RIS System Information
    ris_url: Optional[HttpUrl] = None
    ris_provider: RISProvider = RISProvider.UNKNOWN
    ris_accessible: bool = False
    last_discovery_check: Optional[datetime] = None
    discovery_status: ScrapingStatus = ScrapingStatus.PENDING
    
    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    scraping_notes: Optional[str] = None


class Meeting(BaseModel):
    """Municipal meeting data model."""
    municipality_name: str
    meeting_id: Optional[str] = None  # System-specific meeting ID
    
    # Meeting Details
    title: str
    meeting_type: MeetingType
    date: datetime
    location: Optional[str] = None
    committee: Optional[str] = None
    
    # Status
    is_public: bool = True
    is_cancelled: bool = False
    status: str = "scheduled"  # scheduled, completed, cancelled
    
    # System Information
    source_url: Optional[HttpUrl] = None
    ris_provider: RISProvider = RISProvider.UNKNOWN
    
    # Scraping Status
    scraping_status: ScrapingStatus = ScrapingStatus.PENDING
    last_scraped: Optional[datetime] = None
    
    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class MeetingDocument(BaseModel):
    """Document associated with a meeting."""
    municipality_name: str
    meeting_id: Optional[str] = None
    
    # Document Details
    title: str
    document_type: DocumentType
    file_name: Optional[str] = None
    file_size_bytes: Optional[int] = None
    file_format: Optional[str] = None  # pdf, doc, docx, etc.
    
    # Access Information
    download_url: Optional[HttpUrl] = None
    direct_access: bool = False
    requires_session: bool = False
    
    # Content
    raw_text: Optional[str] = None
    clean_text: Optional[str] = None
    markdown_text: Optional[str] = None
    
    # Storage
    local_path: Optional[str] = None
    cloud_storage_path: Optional[str] = None
    
    # Status
    download_status: ScrapingStatus = ScrapingStatus.PENDING
    last_downloaded: Optional[datetime] = None
    
    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Protocol(BaseModel):
    """Meeting protocol with extracted content."""
    municipality_name: str
    meeting_id: Optional[str] = None
    document_id: Optional[str] = None
    
    # Protocol Details
    title: str
    meeting_date: datetime
    meeting_type: MeetingType
    attendees: List[str] = Field(default_factory=list)
    topics: List[str] = Field(default_factory=list)
    decisions: List[str] = Field(default_factory=list)
    
    # Content
    full_text: str
    summary: Optional[str] = None
    key_points: List[str] = Field(default_factory=list)
    
    # Source Document
    source_document: Optional[MeetingDocument] = None
    
    # Processing Status
    processed: bool = False
    processing_date: Optional[datetime] = None
    processing_notes: Optional[str] = None
    
    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class DiscoveryResult(BaseModel):
    """Result of municipality RIS discovery."""
    municipality: Municipality
    discovered_urls: List[HttpUrl] = Field(default_factory=list)
    verified_url: Optional[HttpUrl] = None
    provider_detected: RISProvider = RISProvider.UNKNOWN
    accessibility_test_passed: bool = False
    discovery_method: str = "unknown"
    discovery_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error_messages: List[str] = Field(default_factory=list)


class ScrapingSession(BaseModel):
    """Scraping session tracking."""
    session_id: str
    municipality_name: str
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    
    # Statistics
    meetings_found: int = 0
    documents_found: int = 0
    documents_downloaded: int = 0
    protocols_extracted: int = 0
    errors_encountered: int = 0
    
    # Results
    meetings: List[Meeting] = Field(default_factory=list)
    documents: List[MeetingDocument] = Field(default_factory=list)
    protocols: List[Protocol] = Field(default_factory=list)
    
    # Status
    status: ScrapingStatus = ScrapingStatus.PENDING
    error_log: List[str] = Field(default_factory=list)
    notes: Optional[str] = None