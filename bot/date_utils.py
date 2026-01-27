"""Date utilities for parsing and calculating report periods."""
import re
from datetime import datetime, timedelta
from typing import Optional, Tuple


def parse_date(date_str: str) -> Optional[datetime]:
    """
    Parses a date string in DD.MM.YYYY format.
    
    Args:
        date_str: Date string in DD.MM.YYYY format
        
    Returns:
        datetime object or None if parsing fails
    """
    try:
        # Clean input
        date_str = date_str.strip()
        # Parse DD.MM.YYYY
        return datetime.strptime(date_str, "%d.%m.%Y")
    except (ValueError, AttributeError):
        return None


def parse_period(period_str: str) -> Optional[Tuple[datetime, datetime]]:
    """
    Parses a period string in format "с DD.MM.YYYY по DD.MM.YYYY".
    
    Args:
        period_str: Period string in format "с DD.MM.YYYY по DD.MM.YYYY"
        
    Returns:
        Tuple of (start_date, end_date) or None if parsing fails
    """
    try:
        # Clean input
        period_str = period_str.strip().lower()
        
        # Pattern: "с DD.MM.YYYY по DD.MM.YYYY"
        pattern = r'с\s*(\d{2}\.\d{2}\.\d{4})\s*по\s*(\d{2}\.\d{2}\.\d{4})'
        match = re.search(pattern, period_str)
        
        if not match:
            return None
            
        start_str = match.group(1)
        end_str = match.group(2)
        
        start_date = parse_date(start_str)
        end_date = parse_date(end_str)
        
        if not start_date or not end_date:
            return None
            
        # Set time ranges: start of day to end of day
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # Validate that start is before end
        if start_date > end_date:
            return None
            
        return (start_date, end_date)
        
    except (ValueError, AttributeError):
        return None


def get_preset_period(period_type: str) -> Tuple[datetime, datetime]:
    """
    Calculates preset period dates.
    
    Args:
        period_type: One of "today", "yesterday", "week", "month"
        
    Returns:
        Tuple of (start_date, end_date)
    """
    now = datetime.now()
    
    if period_type == "today":
        # From start of today to current moment
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
        
    elif period_type == "yesterday":
        # All of yesterday
        yesterday = now - timedelta(days=1)
        start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        
    elif period_type == "week":
        # Last 7 days (including today)
        start = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
        
    elif period_type == "month":
        # Last 30 days (including today)
        start = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
        
    else:
        # Default to all time
        start = datetime(2000, 1, 1)
        end = now
    
    return (start, end)


def format_date_range(start: datetime, end: datetime) -> str:
    """
    Formats a date range for display to the user.
    
    Args:
        start: Start datetime
        end: End datetime
        
    Returns:
        Formatted string like "27.01.2026" or "20.01.2026 - 27.01.2026"
    """
    start_str = start.strftime("%d.%m.%Y")
    end_str = end.strftime("%d.%m.%Y")
    
    # If same day, show only one date
    if start_str == end_str:
        return start_str
    
    return f"{start_str} - {end_str}"
