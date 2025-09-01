import csv
import uuid

import ast
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
from io import StringIO
import asyncio

from .supabase_client import SupabaseClient
from .logo_matcher import LogoMatcher
from .xlsx_processor import XLSXProcessor
from config import RateLimitConfig, ProcessingConfig
from utils.logging_config import get_logger

logger = get_logger(__name__)

class CSVProcessor:
    """Service for processing CSV files and inserting smartjects into database"""

    def __init__(self, supabase_client: SupabaseClient):
        self.supabase = supabase_client
        self.logo_matcher = None
        self.xlsx_processor = XLSXProcessor()

        # Fetch reference data
        self.industries = self.supabase.fetch_reference_table("industries")
        self.audience = self.supabase.fetch_reference_table("audience")
        self.business_functions = self.supabase.fetch_reference_table("business_functions")

        # Create lookup dictionaries
        self.industries_dict = {item["name"].lower(): item for item in self.industries}
        self.audience_dict = {item["name"].lower(): item for item in self.audience}
        self.functions_dict = {item["name"].lower(): item for item in self.business_functions}

        # Statistics
        self.reset_stats()

    def set_logo_matcher(self, logo_matcher: 'LogoMatcher'):
        """Set logo matcher instance"""
        self.logo_matcher = logo_matcher

    def reset_stats(self):
        """Reset processing statistics"""
        self.stats = {
            'total': 0,
            'processed': 0,
            'skipped_not_relevant': 0,
            'skipped_exists': 0,
            'skipped_no_tags': 0,
            'errors': 0,
            'with_logos': 0
        }

    def parse_csv_array(self, value: str) -> List[str]:
        """Parse CSV array string to Python list"""
        if not value or value.strip() == '':
            return []

        try:
            # Try to parse as Python list
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed]
        except:
            pass

        # If that fails, split by commas
        return [item.strip().strip('"').strip("'") for item in value.split(',') if item.strip()]

    def map_tags_simple(self, industries_input: List[str], audience_input: List[str],
                       functions_input: List[str]) -> Dict:
        """Simple tag mapping by name"""

        # Map input tags
        mapped_industries = []
        for tag in industries_input:
            if tag.lower() in self.industries_dict:
                mapped_industries.append(self.industries_dict[tag.lower()]["name"])

        mapped_audience = []
        for tag in audience_input:
            if tag.lower() in self.audience_dict:
                mapped_audience.append(self.audience_dict[tag.lower()]["name"])

        mapped_functions = []
        for tag in functions_input:
            if tag.lower() in self.functions_dict:
                mapped_functions.append(self.functions_dict[tag.lower()]["name"])

        return {
            "industries": mapped_industries,
            "audience": mapped_audience,
            "businessFunctions": mapped_functions
        }

    def map_names_to_full(self, tags: List[str], reference_dict: Dict[str, Dict]) -> List[Dict]:
        """Map tag names to full objects with IDs"""
        mapped = []
        for tag in tags:
            matched_item = reference_dict.get(tag.lower())
            if matched_item:
                mapped.append({"id": matched_item["id"], "name": matched_item["name"]})
        return mapped

    def parse_date(self, date_str: str) -> str:
        """Parse date to ISO format"""
        if not date_str:
            return datetime.now(timezone.utc).isoformat()

        try:
            # Try to parse various date formats
            if 'T' in date_str:
                return date_str  # Already in ISO format

            # Parse format from CSV
            dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
            return dt.isoformat()
        except:
            return datetime.now(timezone.utc).isoformat()

    def read_csv_data(self, file_content: str) -> List[Dict]:
        """Read data from CSV file content"""
        smartjects = []

        # Use StringIO to read from string content
        csvfile = StringIO(file_content)

        # Detect delimiter
        first_line = csvfile.readline()
        delimiter = ';' if ';' in first_line else ','
        csvfile.seek(0)

        reader = csv.DictReader(csvfile, delimiter=delimiter)

        for row in reader:
            # Skip empty rows
            if not row.get('name') or not row.get('name').strip():
                continue

            smartject = {
                'url': row.get('url', '').strip(),
                'publish_date': row.get('publish_date', '').strip(),
                'summarized': row.get('summarized', '').strip(),
                'name': row.get('name', '').strip(),
                'mission': row.get('mission ', '').strip(),  # Note the space in the header
                'problematics': row.get('problematics', '').strip(),
                'scope': row.get('scope', '').strip(),
                'audience': row.get('audience', '').strip(),
                'how_it_works': row.get('how it works', '').strip(),
                'architecture': row.get('architecture', '').strip(),
                'innovation': row.get('innovation', '').strip(),
                'use_case': row.get('use case', '').strip(),
                'industries': row.get('industries', '').strip(),
                'functions': row.get('functions', '').strip(),
                'link': row.get('link', '').strip(),
                'date': row.get('date', '').strip(),
                'team': row.get('team', '').strip()
            }

            smartjects.append(smartject)

        return smartjects

    async def process_xlsx_content(self, file_content: bytes, progress_callback=None) -> Dict:
        """Process XLSX file content by converting it to CSV and then processing"""
        try:
            logger.info("Processing XLSX file...")

            # Validate XLSX structure first
            validation_info = self.xlsx_processor.validate_xlsx_structure(file_content)
            if not validation_info['valid']:
                error_msg = validation_info['error']
                logger.error(f"XLSX validation failed: {error_msg}")
                raise ValueError(f"Invalid XLSX file: {error_msg}")

            logger.info(f"XLSX validation passed. Available sheets: {validation_info['sheets']}")
            logger.info(f"Smartjects sheet has {validation_info.get('smartjects_rows', 0)} rows")

            # Convert XLSX to CSV format
            csv_content = self.xlsx_processor.read_xlsx_content(file_content)
            if not csv_content:
                raise ValueError("Failed to convert XLSX to CSV format")

            logger.info("Successfully converted XLSX to CSV format")

            # Process the converted CSV content using existing logic
            return await self.process_csv_content(csv_content, progress_callback)

        except Exception as e:
            logger.error(f"Error processing XLSX file: {e}")
            raise Exception(f"Failed to process XLSX file: {str(e)}")

    async def process_csv_content(self, file_content: str, progress_callback=None) -> Dict:
        """Process CSV file content and insert smartjects"""
        self.reset_stats()

        # Fetch existing titles
        logger.info("Fetching existing titles from database...")
        existing_titles = self.supabase.fetch_existing_titles()
        logger.info(f"Found {len(existing_titles)} existing titles in database")
        logger.info(existing_titles)

        # Debug: log sample of existing titles
        if existing_titles:
            logger.debug(f"Sample of existing titles (first 10): {list(existing_titles)[:10]}")
        else:
            logger.warning("⚠️ WARNING: existing_titles is EMPTY! All smartjects will be treated as new!")
            logger.warning("This is why duplicates are being added!")

        # Read CSV data
        smartjects_data = self.read_csv_data(file_content)
        self.stats['total'] = len(smartjects_data)

        logger.info(f"Found {len(smartjects_data)} smartjects in CSV")

        results = []

        # Process in batches to avoid rate limits
        batch_size = RateLimitConfig.BATCH_SIZE
        batch_delay = RateLimitConfig.BATCH_DELAY

        for batch_start in range(0, len(smartjects_data), batch_size):
            batch_end = min(batch_start + batch_size, len(smartjects_data))
            batch = smartjects_data[batch_start:batch_end]

            for batch_idx, smartject_data in enumerate(batch):
                idx = batch_start + batch_idx
                title = smartject_data['name']

                # Send progress update
                if progress_callback:
                    await progress_callback(idx + 1, self.stats['total'], title)

                # Check if article is not relevant
                if smartject_data['summarized'] == 'NO (not relevant)':
                    # logger.info(f"Skipped (not relevant): {title}")
                    self.stats['skipped_not_relevant'] += 1
                    results.append({
                        'title': title,
                        'status': 'skipped',
                        'reason': 'not relevant'
                    })
                    continue

                # Check if already exists
                # Normalize title for comparison (same as in fetch_existing_titles)
                normalized_title = title.strip().lower()

                # Debug logging for duplicate check
                logger.debug(f"Checking title: '{title}'")
                logger.debug(f"Normalized title: '{normalized_title}'")
                logger.debug(f"Is in existing_titles: {normalized_title in existing_titles}")
                logger.debug(f"Total existing_titles count: {len(existing_titles)}")

                if normalized_title in existing_titles:
                    logger.info(f"Skipped (already exists): '{title}' (normalized: '{normalized_title}')")
                    self.stats['skipped_exists'] += 1
                    results.append({
                        'title': title,
                        'status': 'skipped',
                        'reason': 'already exists'
                    })
                    continue

                # Additional check for empty or whitespace-only titles
                if not normalized_title:
                    logger.warning(f"Skipped (empty title): '{title}'")
                    self.stats['skipped_no_tags'] += 1
                    results.append({
                        'title': title,
                        'status': 'skipped',
                        'reason': 'empty title'
                    })
                    continue

                # logger.info(f"Processing: {title}")

                try:
                    # Parse tags from CSV
                    industries_input = self.parse_csv_array(smartject_data['industries'])
                    audience_input = self.parse_csv_array(smartject_data['audience'])
                    functions_input = self.parse_csv_array(smartject_data['functions'])
                    team = self.parse_csv_array(smartject_data['team'])

                    # Map tags
                    mapped_tags = self.map_tags_simple(industries_input, audience_input, functions_input)

                    # Map tags to full objects with IDs
                    industries_mapped = self.map_names_to_full(
                        mapped_tags.get('industries', []),
                        self.industries_dict
                    )
                    audience_mapped = self.map_names_to_full(
                        mapped_tags.get('audience', []),
                        self.audience_dict
                    )
                    functions_mapped = self.map_names_to_full(
                        mapped_tags.get('businessFunctions', []),
                        self.functions_dict
                    )

                    # Create smartject
                    smartject_id = str(uuid.uuid4())
                    created_at = self.parse_date(smartject_data['publish_date'])
                    updated_at = created_at

                    # Log the title we're about to insert (for debugging duplicates)
                    logger.debug(f"Preparing to insert smartject: '{title}' (ID: {smartject_id})")

                    # Find logo for university if logo_matcher is available
                    logo_url = None  # Default to None (no logo)
                    logo_match_type = None

                    if self.logo_matcher and team:
                        logger.debug(f"Searching for logo for '{title}' with team: {team}")
                        logo_result = self.logo_matcher.find_matching_logo(team)
                        if logo_result:
                            matched_university, matched_logo_url, match_type = logo_result
                            logo_url = matched_logo_url
                            logo_match_type = match_type
                            self.stats['with_logos'] += 1
                            logger.info(f"Found logo for '{title}': matched '{matched_university}' ({match_type})")
                            logger.debug(f"Logo URL: {logo_url}")
                        else:
                            logger.debug(f"No logo found for '{title}' with team: {team}")
                    else:
                        if not self.logo_matcher:
                            logger.debug(f"Logo matcher not available for '{title}'")
                        if not team:
                            logger.debug(f"No team data for '{title}'")

                    # Prepare data for main table
                    core_data = {
                        "id": smartject_id,
                        "title": title,
                        "image_url": logo_url if logo_url else None,  # Explicitly set to None if no logo found
                        "mission": smartject_data['mission'],
                        "problematics": smartject_data['problematics'],
                        "scope": smartject_data['scope'],
                        "audience": smartject_data['audience'],
                        "how_it_works": smartject_data['how_it_works'],
                        "architecture": smartject_data['architecture'],
                        "team": team,
                        "innovation": smartject_data['innovation'],
                        "use_case": smartject_data['use_case'],
                        "created_at": created_at,
                        "updated_at": updated_at,
                        "research_papers": [{"title": title, "url": smartject_data['link']}] if smartject_data['link'] else []
                    }

                    # Insert into main table
                    if not self.supabase.insert_smartject(core_data):
                        raise Exception("Failed to insert smartject")

                    # Insert relations
                    if industries_mapped:
                        industry_relations = [
                            {"smartject_id": smartject_id, "industry_id": item["id"]}
                            for item in industries_mapped
                        ]
                        if not self.supabase.insert_smartject_industries(industry_relations):
                            raise Exception("Failed to insert industry relations")

                    if audience_mapped:
                        audience_relations = [
                            {"smartject_id": smartject_id, "audience_id": item["id"]}
                            for item in audience_mapped
                        ]
                        if not self.supabase.insert_smartject_audience(audience_relations):
                            raise Exception("Failed to insert audience relations")

                    if functions_mapped:
                        function_relations = [
                            {"smartject_id": smartject_id, "function_id": item["id"]}
                            for item in functions_mapped
                        ]
                        if not self.supabase.insert_smartject_functions(function_relations):
                            raise Exception("Failed to insert function relations")

                    # Teams will be synced in batch at the end

                    self.stats['processed'] += 1

                    # Add the normalized title to existing_titles to prevent duplicates within the same batch
                    normalized_title = title.strip().lower()
                    existing_titles.add(normalized_title)
                    logger.debug(f"Added '{normalized_title}' to existing_titles set (now contains {len(existing_titles)} titles)")

                    results.append({
                        'title': title,
                        'status': 'success',
                        'industries': [item['name'] for item in industries_mapped],
                        'audience': [item['name'] for item in audience_mapped],
                        'functions': [item['name'] for item in functions_mapped],
                        'logo_match': logo_match_type
                    })

                    # logger.info(f"Successfully inserted: {title}")

                except Exception as e:
                    logger.error(f"Error processing {title}: {e}")
                    self.stats['errors'] += 1
                    results.append({
                        'title': title,
                        'status': 'error',
                        'error': str(e)
                    })

            # Add delay between batches to avoid rate limits
            if batch_end < len(smartjects_data) and batch_delay > 0:
                await asyncio.sleep(batch_delay)

        # Batch sync all teams at the end
        if self.stats['processed'] > 0:
            logger.info("Performing batch teams synchronization...")
            try:
                team_sync_stats = self.supabase.batch_sync_all_teams()
                logger.info(f"Teams sync completed: {team_sync_stats['new_teams']} new teams, "
                           f"{team_sync_stats['new_relations']} relations created")
            except Exception as e:
                logger.error(f"Error during batch teams sync: {e}")

        return {
            'stats': self.stats,
            'results': results
        }

    def get_summary(self) -> str:
        """Get processing summary"""
        return (
            f"📊 Processing Summary:\n"
            f"Total records: {self.stats['total']}\n"
            f"✅ Processed: {self.stats['processed']}\n"
            f"🎓 With logos: {self.stats['with_logos']}\n"
            f"⏭️  Skipped (not relevant): {self.stats['skipped_not_relevant']}\n"
            f"⏭️  Skipped (exists): {self.stats['skipped_exists']}\n"
        )
