"""Protocol scraper for extracting meeting documents from German municipal RIS systems."""

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse
import aiohttp
import aiofiles
from pathlib import Path

from ..scraper import WebsiteScraper, BrowserConfig, ScrapedContent
from .data_models import (
    Municipality, Meeting, MeetingDocument, Protocol, ScrapingSession,
    MeetingType, DocumentType, RISProvider, ScrapingStatus
)

logger = logging.getLogger(__name__)


class ProtocolScraper:
    """Scraper for extracting meeting protocols from German municipal RIS systems."""
    
    # Mapping German meeting types to enum values
    MEETING_TYPE_MAPPING = {
        'gemeinderat': MeetingType.GEMEINDERAT,
        'stadtrat': MeetingType.STADTRAT,
        'gemeindevertretung': MeetingType.GEMEINDEVERTRETUNG,
        'ausschuss': MeetingType.AUSSCHUSS,
        'finanzausschuss': MeetingType.FINANZAUSSCHUSS,
        'bauausschuss': MeetingType.BAUAUSSCHUSS,
        'hauptausschuss': MeetingType.HAUPTAUSSCHUSS,
        'jugendausschuss': MeetingType.JUGENDAUSSCHUSS,
        'sozialausschuss': MeetingType.SOZIALAUSSCHUSS
    }
    
    # Mapping German document types to enum values
    DOCUMENT_TYPE_MAPPING = {
        'protokoll': DocumentType.PROTOKOLL,
        'niederschrift': DocumentType.PROTOKOLL,
        'tagesordnung': DocumentType.TAGESORDNUNG,
        'einladung': DocumentType.EINLADUNG,
        'vorlage': DocumentType.VORLAGE,
        'beschluss': DocumentType.BESCHLUSS,
        'anlage': DocumentType.ANLAGE
    }
    
    # Common file extensions for meeting documents
    DOCUMENT_EXTENSIONS = ['.pdf', '.doc', '.docx', '.txt', '.html']
    
    def __init__(
        self,
        scraper: Optional[WebsiteScraper] = None,
        download_dir: str = "./downloads/protocols"
    ):
        self.scraper = scraper or WebsiteScraper(
            max_concurrent=2,  # Be respectful to municipal servers
            requests_per_second=0.5  # Conservative rate limiting
        )
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            headers={'User-Agent': 'Mozilla/5.0 (compatible; MunicipalProtocolScraper/1.0)'}
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            
    async def scrape_municipality_protocols(
        self,
        municipality: Municipality,
        max_meetings: int = 50,
        download_documents: bool = True
    ) -> ScrapingSession:
        """Scrape all available protocols for a municipality."""
        session = ScrapingSession(
            session_id=f"{municipality.name}_{int(datetime.now().timestamp())}",
            municipality_name=municipality.name,
            status=ScrapingStatus.PENDING
        )
        
        try:
            if not municipality.ris_url or not municipality.ris_accessible:
                raise ValueError(f"Municipality {municipality.name} has no accessible RIS URL")
                
            session.status = ScrapingStatus.DISCOVERED
            logger.info(f"Starting protocol scraping for {municipality.name} at {municipality.ris_url}")
            
            # Step 1: Discover meetings
            meetings = await self._discover_meetings(municipality, max_meetings)
            session.meetings = meetings
            session.meetings_found = len(meetings)
            
            # Step 2: Extract documents from meetings
            all_documents = []
            for meeting in meetings:
                try:
                    documents = await self._extract_meeting_documents(meeting, municipality)
                    all_documents.extend(documents)
                except Exception as e:
                    error_msg = f"Failed to extract documents for meeting {meeting.title}: {e}"
                    session.error_log.append(error_msg)
                    logger.warning(error_msg)
                    
            session.documents = all_documents
            session.documents_found = len(all_documents)
            
            # Step 3: Download documents if requested
            if download_documents:
                downloaded_docs = []
                for doc in all_documents:
                    try:
                        if await self._download_document(doc, municipality):
                            downloaded_docs.append(doc)
                            session.documents_downloaded += 1
                    except Exception as e:
                        error_msg = f"Failed to download document {doc.title}: {e}"
                        session.error_log.append(error_msg)
                        logger.warning(error_msg)
                        
                # Step 4: Extract protocol text from downloaded documents
                protocols = []
                for doc in downloaded_docs:
                    if doc.document_type == DocumentType.PROTOKOLL and doc.local_path:
                        try:
                            protocol = await self._extract_protocol_content(doc, municipality)
                            if protocol:
                                protocols.append(protocol)
                                session.protocols_extracted += 1
                        except Exception as e:
                            error_msg = f"Failed to extract protocol from {doc.title}: {e}"
                            session.error_log.append(error_msg)
                            logger.warning(error_msg)
                            
                session.protocols = protocols
                
            session.status = ScrapingStatus.SCRAPED
            session.completed_at = datetime.now(timezone.utc)
            
        except Exception as e:
            session.status = ScrapingStatus.FAILED
            session.error_log.append(f"Scraping failed: {str(e)}")
            session.completed_at = datetime.now(timezone.utc)
            logger.error(f"Failed to scrape municipality {municipality.name}: {e}")
            
        session.errors_encountered = len(session.error_log)
        return session
        
    async def _discover_meetings(
        self,
        municipality: Municipality,
        max_meetings: int
    ) -> List[Meeting]:
        """Discover meetings from the municipality's RIS system."""
        meetings = []
        
        if municipality.ris_provider == RISProvider.REGISAFE:
            meetings = await self._discover_regisafe_meetings(municipality, max_meetings)
        elif municipality.ris_provider == RISProvider.SD_NET:
            meetings = await self._discover_sdnet_meetings(municipality, max_meetings)
        else:
            # Generic approach for unknown providers
            meetings = await self._discover_generic_meetings(municipality, max_meetings)
            
        logger.info(f"Discovered {len(meetings)} meetings for {municipality.name}")
        return meetings
        
    async def _discover_regisafe_meetings(
        self,
        municipality: Municipality,
        max_meetings: int
    ) -> List[Meeting]:
        """Discover meetings from regisafe-based systems."""
        meetings = []
        
        try:
            # Scrape the main RIS page
            content = await self.scraper.scrape_single_url(
                str(municipality.ris_url),
                BrowserConfig(headless=True, timeout=30000)
            )
            
            if content.error:
                logger.error(f"Failed to access RIS for {municipality.name}: {content.error}")
                return meetings
                
            # Extract meeting links and information
            meeting_links = self._extract_regisafe_meeting_links(content)
            
            # Process each meeting link
            for i, (link, preliminary_info) in enumerate(meeting_links[:max_meetings]):
                try:
                    meeting = await self._scrape_meeting_details(link, preliminary_info, municipality)
                    if meeting:
                        meetings.append(meeting)
                except Exception as e:
                    logger.warning(f"Failed to scrape meeting details from {link}: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to discover regisafe meetings: {e}")
            
        return meetings
        
    async def _discover_generic_meetings(
        self,
        municipality: Municipality, 
        max_meetings: int
    ) -> List[Meeting]:
        """Generic meeting discovery for unknown RIS providers."""
        meetings = []
        
        try:
            content = await self.scraper.scrape_single_url(
                str(municipality.ris_url),
                BrowserConfig(headless=True, timeout=30000)
            )
            
            if content.error:
                return meetings
                
            # Look for meeting-related links in the content
            potential_meeting_links = self._extract_potential_meeting_links(content)
            
            # Test each link to see if it contains meeting information
            for link in potential_meeting_links[:max_meetings]:
                try:
                    meeting_content = await self.scraper.scrape_single_url(
                        link,
                        BrowserConfig(headless=True, timeout=20000)
                    )
                    
                    if not meeting_content.error:
                        meeting = self._parse_generic_meeting_info(meeting_content, municipality)
                        if meeting:
                            meetings.append(meeting)
                            
                except Exception as e:
                    logger.warning(f"Failed to check potential meeting link {link}: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to discover generic meetings: {e}")
            
        return meetings
        
    def _extract_regisafe_meeting_links(self, content: ScrapedContent) -> List[Tuple[str, Dict]]:
        """Extract meeting links from regisafe system."""
        meeting_links = []
        
        # Look for typical regisafe meeting link patterns
        for link in content.links:
            if any(pattern in link.lower() for pattern in ['si010', 'to010', 'session']):
                # Extract preliminary meeting info from the link/context if possible
                preliminary_info = {
                    'source_url': link,
                    'discovered_at': datetime.now(timezone.utc)
                }
                meeting_links.append((link, preliminary_info))
                
        return meeting_links
        
    def _extract_potential_meeting_links(self, content: ScrapedContent) -> List[str]:
        """Extract potential meeting links from generic systems."""
        potential_links = []
        
        # Look for links containing meeting-related keywords
        meeting_keywords = [
            'sitzung', 'meeting', 'protokoll', 'tagesordnung', 
            'gemeinderat', 'stadtrat', 'ausschuss'
        ]
        
        for link in content.links:
            link_lower = link.lower()
            if any(keyword in link_lower for keyword in meeting_keywords):
                potential_links.append(link)
                
        return potential_links
        
    async def _scrape_meeting_details(
        self,
        meeting_url: str,
        preliminary_info: Dict,
        municipality: Municipality
    ) -> Optional[Meeting]:
        """Scrape detailed meeting information from a meeting page."""
        try:
            content = await self.scraper.scrape_single_url(
                meeting_url,
                BrowserConfig(headless=True, timeout=20000)
            )
            
            if content.error:
                return None
                
            return self._parse_meeting_content(content, municipality)
            
        except Exception as e:
            logger.warning(f"Failed to scrape meeting details: {e}")
            return None
            
    def _parse_meeting_content(
        self,
        content: ScrapedContent,
        municipality: Municipality
    ) -> Optional[Meeting]:
        """Parse meeting information from scraped content."""
        try:
            # Extract meeting title
            title = content.title or "Unbekannte Sitzung"
            
            # Try to extract meeting date (this is provider-specific)
            meeting_date = self._extract_meeting_date(content.text)
            if not meeting_date:
                meeting_date = datetime.now()  # Fallback
                
            # Determine meeting type
            meeting_type = self._determine_meeting_type(title + " " + content.text)
            
            meeting = Meeting(
                municipality_name=municipality.name,
                title=title,
                meeting_type=meeting_type,
                date=meeting_date,
                source_url=content.url,
                ris_provider=municipality.ris_provider,
                scraping_status=ScrapingStatus.DISCOVERED
            )
            
            return meeting
            
        except Exception as e:
            logger.warning(f"Failed to parse meeting content: {e}")
            return None
            
    def _parse_generic_meeting_info(
        self,
        content: ScrapedContent,
        municipality: Municipality
    ) -> Optional[Meeting]:
        """Parse meeting info from generic/unknown systems."""
        return self._parse_meeting_content(content, municipality)
        
    async def _extract_meeting_documents(
        self,
        meeting: Meeting,
        municipality: Municipality
    ) -> List[MeetingDocument]:
        """Extract documents associated with a meeting."""
        documents = []
        
        if not meeting.source_url:
            return documents
            
        try:
            # Re-scrape the meeting page to look for document links
            content = await self.scraper.scrape_single_url(
                str(meeting.source_url),
                BrowserConfig(headless=True, timeout=20000)
            )
            
            if content.error:
                return documents
                
            # Extract document links
            doc_links = self._extract_document_links(content)
            
            for link_info in doc_links:
                doc = MeetingDocument(
                    municipality_name=municipality.name,
                    meeting_id=meeting.meeting_id,
                    title=link_info.get('title', 'Unbekanntes Dokument'),
                    document_type=link_info.get('document_type', DocumentType.ANDERE),
                    file_name=link_info.get('file_name'),
                    file_format=link_info.get('file_format'),
                    download_url=link_info.get('url'),
                    download_status=ScrapingStatus.DISCOVERED
                )
                documents.append(doc)
                
        except Exception as e:
            logger.warning(f"Failed to extract documents for meeting {meeting.title}: {e}")
            
        return documents
        
    def _extract_document_links(self, content: ScrapedContent) -> List[Dict]:
        """Extract document download links from meeting page."""
        doc_links = []
        
        for link in content.links:
            # Check if link points to a document
            if any(ext in link.lower() for ext in self.DOCUMENT_EXTENSIONS):
                # Try to determine document type from filename or context
                doc_type = self._guess_document_type(link)
                file_format = self._extract_file_extension(link)
                
                doc_info = {
                    'url': link,
                    'title': self._extract_filename_from_url(link),
                    'document_type': doc_type,
                    'file_format': file_format,
                    'file_name': self._extract_filename_from_url(link)
                }
                doc_links.append(doc_info)
                
        return doc_links
        
    async def _download_document(
        self,
        document: MeetingDocument,
        municipality: Municipality
    ) -> bool:
        """Download a meeting document."""
        if not document.download_url or not self.session:
            return False
            
        try:
            # Create municipality-specific download directory
            muni_dir = self.download_dir / municipality.name
            muni_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate local filename
            filename = document.file_name or f"document_{int(datetime.now().timestamp())}"
            if not any(filename.endswith(ext) for ext in self.DOCUMENT_EXTENSIONS):
                filename += f".{document.file_format or 'pdf'}"
                
            local_path = muni_dir / filename
            
            # Download the file
            async with self.session.get(str(document.download_url)) as response:
                if response.status == 200:
                    async with aiofiles.open(local_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            await f.write(chunk)
                            
                    document.local_path = str(local_path)
                    document.file_size_bytes = local_path.stat().st_size
                    document.download_status = ScrapingStatus.SCRAPED
                    document.last_downloaded = datetime.now(timezone.utc)
                    
                    logger.info(f"Downloaded document: {filename}")
                    return True
                else:
                    logger.warning(f"Failed to download {document.download_url}: HTTP {response.status}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error downloading document {document.download_url}: {e}")
            document.download_status = ScrapingStatus.FAILED
            return False
            
    async def _extract_protocol_content(
        self,
        document: MeetingDocument,
        municipality: Municipality
    ) -> Optional[Protocol]:
        """Extract and parse content from a protocol document."""
        if not document.local_path or not Path(document.local_path).exists():
            return None
            
        try:
            # For now, just extract basic metadata
            # Full PDF text extraction would require additional libraries
            protocol = Protocol(
                municipality_name=municipality.name,
                meeting_id=document.meeting_id,
                document_id=str(document.download_url),
                title=document.title,
                meeting_date=datetime.now(),  # Would extract from document
                meeting_type=MeetingType.ANDERE,  # Would extract from document
                full_text="[Text extraction not implemented]",
                source_document=document,
                processed=False
            )
            
            return protocol
            
        except Exception as e:
            logger.error(f"Failed to extract protocol content: {e}")
            return None
            
    # Utility methods
    
    def _extract_meeting_date(self, text: str) -> Optional[datetime]:
        """Extract meeting date from text."""
        # German date patterns
        date_patterns = [
            r'(\d{1,2})\.(\d{1,2})\.(\d{4})',  # DD.MM.YYYY
            r'(\d{4})-(\d{1,2})-(\d{1,2})'     # YYYY-MM-DD
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    if '.' in pattern:
                        day, month, year = match.groups()
                        return datetime(int(year), int(month), int(day))
                    else:
                        year, month, day = match.groups()
                        return datetime(int(year), int(month), int(day))
                except ValueError:
                    continue
                    
        return None
        
    def _determine_meeting_type(self, text: str) -> MeetingType:
        """Determine meeting type from text content."""
        text_lower = text.lower()
        
        for keyword, meeting_type in self.MEETING_TYPE_MAPPING.items():
            if keyword in text_lower:
                return meeting_type
                
        return MeetingType.ANDERE
        
    def _guess_document_type(self, url: str) -> DocumentType:
        """Guess document type from URL or filename."""
        url_lower = url.lower()
        
        for keyword, doc_type in self.DOCUMENT_TYPE_MAPPING.items():
            if keyword in url_lower:
                return doc_type
                
        return DocumentType.ANDERE
        
    def _extract_file_extension(self, url: str) -> Optional[str]:
        """Extract file extension from URL."""
        for ext in self.DOCUMENT_EXTENSIONS:
            if url.lower().endswith(ext):
                return ext[1:]  # Remove the dot
        return None
        
    def _extract_filename_from_url(self, url: str) -> str:
        """Extract filename from URL."""
        try:
            parsed = urlparse(url)
            filename = parsed.path.split('/')[-1]
            return filename if filename else "document"
        except:
            return "document"
            
    async def _discover_sdnet_meetings(
        self,
        municipality: Municipality,
        max_meetings: int
    ) -> List[Meeting]:
        """Discover meetings from SD.NET-based systems."""
        # Placeholder for SD.NET-specific implementation
        return await self._discover_generic_meetings(municipality, max_meetings)