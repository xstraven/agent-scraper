"""
German Municipal Protocol Scraper

A system for automatically discovering and scraping council meeting protocols
from German municipalities (Kommune/Gemeinde level) across Germany.
"""

from .data_models import Municipality, Meeting, Protocol, MeetingDocument
from .target_discovery import TargetDiscovery
from .protocol_scraper import ProtocolScraper

__all__ = [
    "Municipality", 
    "Meeting", 
    "Protocol", 
    "MeetingDocument",
    "TargetDiscovery",
    "ProtocolScraper"
]