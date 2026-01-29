"""Target discovery system for finding German municipal RIS URLs."""

import asyncio
import logging
import re
from typing import List, Dict, Optional, Set
from urllib.parse import urlparse, urljoin
import aiohttp

from ..scraper import WebsiteScraper, BrowserConfig, ScrapedContent
from .data_models import (
    Municipality, DiscoveryResult, RISProvider, ScrapingStatus,
    GermanState, AdministrativeLevel
)

logger = logging.getLogger(__name__)


class TargetDiscovery:
    """System for discovering German municipal RIS/Sitzungsdienst URLs."""
    
    # Common URL patterns for German municipal RIS systems
    RIS_URL_PATTERNS = [
        "ratsinfo.{domain}",
        "ris.{domain}", 
        "sitzungsdienst.{domain}",
        "ratsinformation.{domain}",
        "buergerinfo.{domain}",
        "sitzungen.{domain}",
        "gemeinderat.{domain}",
        "stadtrat.{domain}",
        "{name}.ratsinfo.de",
        "{name}.ris.de",
        "sitzungsdienst-{name}.de",
        "ratsinfo-{name}.de"
    ]
    
    # Known provider-specific patterns
    PROVIDER_PATTERNS = {
        RISProvider.REGISAFE: [
            "regisafe.de",
            "buergerinfo.de",
            "ratsinfo.de"
        ],
        RISProvider.SD_NET: [
            "sitzungsdienst.net",
            "sd-net.de"
        ],
        RISProvider.SESSIONNET: [
            "sessionnet.org",
            "session-net.org"
        ],
        RISProvider.ALLRIS: [
            "allris.de"
        ]
    }
    
    # Keywords that indicate RIS/council systems
    RIS_KEYWORDS = [
        "ratsinformationssystem",
        "sitzungsdienst", 
        "gemeinderat",
        "stadtrat",
        "gemeindevertretung",
        "sitzungskalender",
        "tagesordnung",
        "protokoll",
        "niederschrift",
        "beschluss",
        "gremienmitglieder",
        "ausschüsse",
        "fraktionen"
    ]
    
    def __init__(self, scraper: Optional[WebsiteScraper] = None):
        self.scraper = scraper or WebsiteScraper(
            max_concurrent=3,
            requests_per_second=1.0
        )
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={'User-Agent': 'Mozilla/5.0 (compatible; MunicipalScraper/1.0)'}
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            
    async def discover_municipality_ris(self, municipality: Municipality) -> DiscoveryResult:
        """Discover RIS URL for a single municipality."""
        result = DiscoveryResult(municipality=municipality)
        
        try:
            # Strategy 1: Pattern-based URL testing
            pattern_urls = await self._test_url_patterns(municipality)
            result.discovered_urls.extend(pattern_urls)
            
            # Strategy 2: Main website analysis  
            website_urls = await self._analyze_main_website(municipality)
            result.discovered_urls.extend(website_urls)
            
            # Strategy 3: Search-based discovery (if no results yet)
            if not result.discovered_urls:
                search_urls = await self._search_based_discovery(municipality)
                result.discovered_urls.extend(search_urls)
            
            # Verify and rank discovered URLs
            if result.discovered_urls:
                verified = await self._verify_ris_urls(result.discovered_urls)
                if verified:
                    result.verified_url = verified[0].url
                    result.provider_detected = verified[0].provider
                    result.accessibility_test_passed = verified[0].accessible
                    
            # Update municipality with discovery results
            if result.verified_url:
                municipality.ris_url = result.verified_url
                municipality.ris_provider = result.provider_detected
                municipality.ris_accessible = result.accessibility_test_passed
                municipality.discovery_status = ScrapingStatus.DISCOVERED
            else:
                municipality.discovery_status = ScrapingStatus.FAILED
                
            result.municipality = municipality
            
        except Exception as e:
            error_msg = f"Discovery failed for {municipality.name}: {str(e)}"
            logger.error(error_msg)
            result.error_messages.append(error_msg)
            municipality.discovery_status = ScrapingStatus.FAILED
            
        return result
        
    async def _test_url_patterns(self, municipality: Municipality) -> List[str]:
        """Test common URL patterns for municipality."""
        candidate_urls = []
        
        # Generate domain variations
        name_clean = self._clean_name_for_url(municipality.name)
        domain_base = f"{name_clean}.de"
        
        # Test pattern variations
        for pattern in self.RIS_URL_PATTERNS:
            if "{domain}" in pattern:
                url = f"https://{pattern.format(domain=domain_base)}"
            elif "{name}" in pattern:
                url = f"https://{pattern.format(name=name_clean)}"
            else:
                continue
                
            candidate_urls.append(url)
            
        # Test URLs in parallel
        accessible_urls = []
        semaphore = asyncio.Semaphore(5)  # Limit concurrent requests
        
        async def test_url(url: str) -> Optional[str]:
            async with semaphore:
                if await self._test_url_accessibility(url):
                    return url
                return None
                
        tasks = [test_url(url) for url in candidate_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, str):
                accessible_urls.append(result)
                
        return accessible_urls
        
    async def _analyze_main_website(self, municipality: Municipality) -> List[str]:
        """Analyze municipality's main website for RIS links."""
        main_urls = self._generate_main_website_urls(municipality)
        discovered_urls = []
        
        for main_url in main_urls:
            try:
                # Scrape the main website
                content = await self.scraper.scrape_single_url(
                    main_url,
                    BrowserConfig(headless=True, timeout=20000)
                )
                
                if content.error or not content.html:
                    continue
                    
                # Look for RIS-related links
                ris_links = self._extract_ris_links(content, main_url)
                discovered_urls.extend(ris_links)
                
            except Exception as e:
                logger.warning(f"Failed to analyze main website {main_url}: {e}")
                
        return discovered_urls
        
    async def _search_based_discovery(self, municipality: Municipality) -> List[str]:
        """Use search engines to find RIS systems (placeholder for future implementation)."""
        # This would implement search engine queries like:
        # "site:*.de ratsinformationssystem {municipality.name}"
        # For now, return empty list
        logger.info(f"Search-based discovery not yet implemented for {municipality.name}")
        return []
        
    async def _verify_ris_urls(self, urls: List[str]) -> List[Dict]:
        """Verify URLs actually contain RIS systems and rank by quality."""
        verified = []
        
        for url in urls:
            try:
                # Scrape the potential RIS page
                content = await self.scraper.scrape_single_url(
                    url,
                    BrowserConfig(headless=True, timeout=15000)
                )
                
                if content.error:
                    continue
                    
                # Analyze content for RIS indicators
                ris_score = self._calculate_ris_score(content)
                provider = self._detect_provider(content)
                
                if ris_score > 0.3:  # Threshold for RIS detection
                    verified.append({
                        'url': url,
                        'score': ris_score,
                        'provider': provider,
                        'accessible': True
                    })
                    
            except Exception as e:
                logger.warning(f"Failed to verify RIS URL {url}: {e}")
                
        # Sort by score (highest first)
        verified.sort(key=lambda x: x['score'], reverse=True)
        return verified
        
    async def _test_url_accessibility(self, url: str) -> bool:
        """Test if URL is accessible."""
        if not self.session:
            return False
            
        try:
            async with self.session.head(url, allow_redirects=True) as response:
                return response.status == 200
        except Exception:
            return False
            
    def _clean_name_for_url(self, name: str) -> str:
        """Clean municipality name for URL generation."""
        # Remove common prefixes/suffixes
        name = re.sub(r'^(Stadt|Gemeinde|Amt)\s+', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s+(Stadt|Gemeinde|Amt)$', '', name, flags=re.IGNORECASE)
        
        # Convert to lowercase and replace special characters
        name = name.lower()
        name = re.sub(r'[äöü]', lambda m: {'ä': 'ae', 'ö': 'oe', 'ü': 'ue'}[m.group()], name)
        name = re.sub(r'[ß]', 'ss', name)
        name = re.sub(r'[^a-z0-9]', '-', name)
        name = re.sub(r'-+', '-', name).strip('-')
        
        return name
        
    def _generate_main_website_urls(self, municipality: Municipality) -> List[str]:
        """Generate possible main website URLs for municipality."""
        name_clean = self._clean_name_for_url(municipality.name)
        
        urls = [
            f"https://{name_clean}.de",
            f"https://www.{name_clean}.de",
            f"https://{name_clean}.com",
            f"https://www.{name_clean}.com"
        ]
        
        # Add state-specific patterns if needed
        return urls
        
    def _extract_ris_links(self, content: ScrapedContent, base_url: str) -> List[str]:
        """Extract RIS-related links from webpage content."""
        ris_links = []
        
        # Look for links containing RIS keywords
        for link in content.links:
            link_lower = link.lower()
            link_text = ""  # Would need to extract link text from HTML
            
            # Check URL and link text for RIS indicators
            for keyword in self.RIS_KEYWORDS:
                if keyword in link_lower or keyword in link_text.lower():
                    # Convert relative URLs to absolute
                    full_url = urljoin(base_url, link)
                    if full_url not in ris_links:
                        ris_links.append(full_url)
                    break
                    
        return ris_links
        
    def _calculate_ris_score(self, content: ScrapedContent) -> float:
        """Calculate likelihood that content represents a RIS system."""
        score = 0.0
        text_lower = (content.text + " " + content.html).lower()
        
        # Check for RIS keywords
        keyword_matches = sum(1 for keyword in self.RIS_KEYWORDS if keyword in text_lower)
        score += min(keyword_matches * 0.2, 0.8)  # Max 0.8 from keywords
        
        # Check for typical RIS page elements
        if "sitzung" in text_lower and "tagesordnung" in text_lower:
            score += 0.3
        if "gemeinderat" in text_lower or "stadtrat" in text_lower:
            score += 0.2
        if "protokoll" in text_lower or "niederschrift" in text_lower:
            score += 0.2
            
        return min(score, 1.0)
        
    def _detect_provider(self, content: ScrapedContent) -> RISProvider:
        """Detect RIS provider from content."""
        url_lower = content.url.lower()
        html_lower = content.html.lower()
        
        # Check URL patterns
        for provider, patterns in self.PROVIDER_PATTERNS.items():
            for pattern in patterns:
                if pattern in url_lower:
                    return provider
                    
        # Check HTML content for provider signatures
        if "regisafe" in html_lower or "buergerinfo" in html_lower:
            return RISProvider.REGISAFE
        elif "sitzungsdienst.net" in html_lower or "sd-net" in html_lower:
            return RISProvider.SD_NET
        elif "sessionnet" in html_lower:
            return RISProvider.SESSIONNET
        elif "allris" in html_lower:
            return RISProvider.ALLRIS
        elif "kommune-aktiv" in html_lower:
            return RISProvider.KOMMUNE_AKTIV
        elif "somacos" in html_lower:
            return RISProvider.SOMACOS
            
        return RISProvider.UNKNOWN
        
    async def discover_multiple_municipalities(
        self,
        municipalities: List[Municipality],
        batch_size: int = 10
    ) -> List[DiscoveryResult]:
        """Discover RIS URLs for multiple municipalities in batches."""
        results = []
        
        for i in range(0, len(municipalities), batch_size):
            batch = municipalities[i:i + batch_size]
            logger.info(f"Processing discovery batch {i//batch_size + 1}/{(len(municipalities) + batch_size - 1)//batch_size}")
            
            # Process batch in parallel
            tasks = [self.discover_municipality_ris(municipality) for municipality in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, DiscoveryResult):
                    results.append(result)
                else:
                    logger.error(f"Discovery task failed: {result}")
                    
            # Small delay between batches to be respectful
            await asyncio.sleep(2)
            
        return results