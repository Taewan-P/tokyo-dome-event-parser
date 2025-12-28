"""
Tokyo Dome Event Parser

Scrapes event data from the Tokyo Dome schedule page and returns
structured data ready for database insertion.
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime
from typing import TypedDict

import requests
from bs4 import BeautifulSoup

# Constants
SCHEDULE_URL = "https://www.tokyo-dome.co.jp/en/dome/event/schedule.html"
D1_DATABASE_NAME = "tokyo-dome-events"


class Event(TypedDict):
    """Event data structure for database storage."""

    date: str  # YYYY-MM-DD format
    name: str  # Event name
    start_time: str  # HH:MM format


def fetch_schedule_html() -> str:
    """Fetch the raw HTML from Tokyo Dome schedule page.

    Returns:
        Raw HTML content of the schedule page.

    Raises:
        requests.RequestException: If the HTTP request fails.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
    }
    response = requests.get(SCHEDULE_URL, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text


def extract_start_time(text: str) -> str | None:
    """Extract start time from event text.

    Handles various formats:
    - 開演 HH:MM (Japanese: performance start)
    - Starts HH:MM
    - Start time HH:MM
    - start HH:MM
    - 開始 HH:MM

    Args:
        text: Event description text containing time information.

    Returns:
        Start time in HH:MM format, or None if not found.
    """
    # Pattern to match various start time formats
    # Looks for 開演, 開始, Starts, start followed by time
    patterns = [
        r"開演\s*(\d{1,2}:\d{2})",  # Japanese: 開演 17:00
        r"開始\s*(\d{1,2}:\d{2})",  # Japanese: 開始 17:00
        r"[Ss]tarts?\s+time\s+(\d{1,2}:\d{2})",  # English: Start time 17:00
        r"[Ss]tarts?\s*(\d{1,2}:\d{2})",  # English: Starts 17:00 or start 17:00
        r"／開演\s*(\d{1,2}:\d{2})",  # With separator: ／開演 17:00
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            time_str = match.group(1)
            # Ensure HH:MM format (pad hour if needed)
            parts = time_str.split(":")
            return f"{int(parts[0]):02d}:{parts[1]}"

    return None


def extract_event_name(cell: BeautifulSoup) -> str | None:
    """Extract event name from a table cell.

    Args:
        cell: BeautifulSoup element containing event information.

    Returns:
        Event name extracted from link text or cell content.
    """
    # Try to find the event name in a link first
    link = cell.find("a")
    if link:
        return link.get_text(strip=True)

    # Fall back to cell text, removing time information
    text = cell.get_text(strip=True)
    # Remove common prefixes like コンサート, 野球, etc.
    prefixes = ["コンサート", "スポーツ", "その他", "野球"]
    for prefix in prefixes:
        if text.startswith(prefix):
            text = text[len(prefix) :].strip()
            break

    return text if text else None


MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]


def find_month_table(soup: BeautifulSoup, year: int, month: int) -> BeautifulSoup | None:
    """Find the table element for a specific month.

    The page uses <p class="c-ttl-set-calender"> to mark each month section.
    The text can be Japanese (e.g., "2025年12月") or English (e.g., "December 2025").

    Args:
        soup: BeautifulSoup parsed HTML.
        year: Target year (e.g., 2025).
        month: Target month (1-12).

    Returns:
        The table element for the specified month, or None if not found.
    """
    # Build patterns to match the month header
    # Japanese format: 2025年12月 (with zero-padded month)
    japanese_pattern = f"{year}年{month:02d}月"
    # English format: December 2025
    english_pattern = f"{MONTH_NAMES[month - 1]} {year}"

    # Find all month header elements
    month_headers = soup.find_all("p", class_="c-ttl-set-calender")

    for header in month_headers:
        # Get the raw HTML to check for Japanese text in comments
        header_html = str(header)
        header_text = header.get_text(strip=True)

        # Check if this header matches our target month
        if japanese_pattern in header_html or japanese_pattern in header_text or english_pattern in header_text:
            # Find the next table after this header
            table = header.find_next("table")
            if table:
                return table

    return None


def parse_events(html: str, year: int, month: int) -> list[Event]:
    """Parse HTML and extract events for the specified month.

    Args:
        html: Raw HTML content of the schedule page.
        year: Target year (e.g., 2025).
        month: Target month (1-12).

    Returns:
        List of Event dictionaries for the specified month.
    """
    soup = BeautifulSoup(html, "lxml")
    events: list[Event] = []

    # Find the table for the target month
    target_table = find_month_table(soup, year, month)

    if not target_table:
        return events

    # Parse table rows
    rows = target_table.find_all("tr")
    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        # First cell contains the date (e.g., "06 (土)")
        date_cell = cells[0].get_text(strip=True)
        date_match = re.match(r"(\d{1,2})", date_cell)
        if not date_match:
            continue

        day = int(date_match.group(1))

        # Second cell contains event information
        event_cell = cells[1] if len(cells) > 1 else None
        if not event_cell:
            continue

        cell_text = event_cell.get_text(strip=True)
        if not cell_text:
            continue

        # Extract event name
        event_name = extract_event_name(event_cell)
        if not event_name:
            continue

        # Extract start time (default to "00:00" if not found)
        start_time = extract_start_time(cell_text) or "00:00"

        # Format date as YYYY-MM-DD
        date_str = f"{year}-{month:02d}-{day:02d}"

        events.append(
            Event(
                date=date_str,
                name=event_name,
                start_time=start_time,
            )
        )

    return events


def get_next_month(year: int, month: int) -> tuple[int, int]:
    """Get the next month's year and month.

    Args:
        year: Current year.
        month: Current month (1-12).

    Returns:
        Tuple of (year, month) for the next month.
    """
    if month == 12:
        return year + 1, 1
    return year, month + 1


def get_events() -> list[Event]:
    """Fetch and parse events for the current month and next month.

    Returns:
        List of Event dictionaries for current and next month.
    """
    now = datetime.now()
    html = fetch_schedule_html()

    # Parse current month
    events = parse_events(html, now.year, now.month)

    # Parse next month
    next_year, next_month = get_next_month(now.year, now.month)
    events.extend(parse_events(html, next_year, next_month))

    return events


def escape_sql_string(value: str) -> str:
    """Escape single quotes in SQL string values.

    Args:
        value: String value to escape.

    Returns:
        Escaped string safe for SQL insertion.
    """
    return value.replace("'", "''")


def generate_upsert_sql(events: list[Event]) -> str:
    """Generate SQL statements for upserting events.

    Uses INSERT OR REPLACE which requires a UNIQUE constraint on (date, name).

    Args:
        events: List of Event dictionaries to upsert.

    Returns:
        SQL statement string for batch upsert.
    """
    if not events:
        return ""

    statements = []
    for event in events:
        date = escape_sql_string(event["date"])
        name = escape_sql_string(event["name"])
        start_time = escape_sql_string(event["start_time"])

        stmt = f"INSERT OR REPLACE INTO events (date, name, start_time) VALUES ('{date}', '{name}', '{start_time}');"
        statements.append(stmt)

    return "\n".join(statements)


def ensure_unique_index() -> bool:
    """Ensure the unique index exists on the events table.

    Returns:
        True if successful, False otherwise.
    """
    sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_date_name ON events(date, name);"

    try:
        result = subprocess.run(
            ["wrangler", "d1", "execute", D1_DATABASE_NAME, "--command", sql, "--remote"],
            capture_output=True,
            text=True,
            check=True,
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"Warning: Could not create unique index: {e.stderr}")
        return False


def save_events_to_d1(events: list[Event]) -> bool:
    """Save events to Cloudflare D1 database using Wrangler CLI.

    Performs upsert based on (date, name) unique constraint.

    Args:
        events: List of Event dictionaries to save.

    Returns:
        True if successful, False otherwise.
    """
    if not events:
        print("No events to save.")
        return True

    # Ensure the unique index exists
    ensure_unique_index()

    # Generate and execute upsert SQL
    sql = generate_upsert_sql(events)

    try:
        result = subprocess.run(
            ["wrangler", "d1", "execute", D1_DATABASE_NAME, "--command", sql, "--remote"],
            capture_output=True,
            text=True,
            check=True,
        )
        print(f"Successfully saved {len(events)} events to D1 database.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error saving to D1: {e.stderr}")
        return False
    except FileNotFoundError:
        print("Error: Wrangler CLI not found. Please install it with: npm install -g wrangler")
        return False


def main() -> None:
    """Main entry point - fetches and optionally saves events for current and next month."""
    parser = argparse.ArgumentParser(
        description="Parse Tokyo Dome events (current + next month) and optionally save to Cloudflare D1."
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Save parsed events to Cloudflare D1 database",
    )
    args = parser.parse_args()

    try:
        events = get_events()
        print(json.dumps(events, ensure_ascii=False, indent=2))
        print(f"\nTotal events found: {len(events)}")

        if args.save:
            print("\nSaving to Cloudflare D1...")
            success = save_events_to_d1(events)
            if not success:
                sys.exit(1)

    except requests.RequestException as e:
        print(f"Error fetching schedule: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing schedule: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
