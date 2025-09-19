#!/usr/bin/env python3
"""
Script to synchronize smartjects from XLSX file with strict audience format validation.
This script:
1. Reads smartjects from XLSX file
2. Validates audience format (must be JSON array)
3. Creates missing audience entries in database
4. Creates/updates smartjects with proper audience relationships
5. Skips smartjects with invalid audience format
"""

import os
import sys
import json
import pandas as pd
import uuid
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
from datetime import datetime
from dotenv import load_dotenv

# Add the bot directory to Python path
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / 'bot'))

from bot.services.supabase_client import SupabaseClient
from bot.utils.logging_config import get_logger

# Load environment variables
load_dotenv()

logger = get_logger(__name__)


class XLSXSmartjectSynchronizer:
    """Synchronizes smartjects from XLSX file with strict audience validation"""

    def __init__(self):
        self.supabase = SupabaseClient()
        self.existing_audiences = {}  # name -> id mapping
        self.existing_industries = {}  # name -> id mapping
        self.existing_functions = {}  # name -> id mapping
        self.existing_smartjects = {}  # title -> id mapping

        self.stats = {
            'total_rows': 0,
            'valid_smartjects': 0,
            'invalid_audience_format': 0,
            'created_smartjects': 0,
            'updated_smartjects': 0,
            'skipped_smartjects': 0,
            'new_audiences': 0,
            'new_industries': 0,
            'new_functions': 0,
            'errors': []
        }

    def parse_comma_separated_audience(self, text: str) -> List[str]:
        """Parse comma-separated audience text into list"""
        import re

        # Remove "and" at the beginning of items
        text = re.sub(r',\s*and\s+', ', ', text)
        text = re.sub(r'\s+and\s+', ', ', text)

        # Split by comma
        items = text.split(',')

        # Clean up each item
        result = []
        for item in items:
            cleaned = item.strip()
            # Remove trailing periods
            cleaned = cleaned.rstrip('.')
            if cleaned:
                result.append(cleaned)

        return result

    def load_existing_data(self):
        """Load existing reference data from database"""
        logger.info("Loading existing reference data...")

        # Load audiences
        audiences = self.supabase.fetch_reference_table("audience")
        for audience in audiences:
            name_lower = audience['name'].lower()
            self.existing_audiences[name_lower] = {
                'id': audience['id'],
                'name': audience['name']
            }
        logger.info(f"  Loaded {len(self.existing_audiences)} existing audiences")

        # Load industries
        industries = self.supabase.fetch_reference_table("industries")
        for industry in industries:
            name_lower = industry['name'].lower()
            self.existing_industries[name_lower] = {
                'id': industry['id'],
                'name': industry['name']
            }
        logger.info(f"  Loaded {len(self.existing_industries)} existing industries")

        # Load business functions
        functions = self.supabase.fetch_reference_table("business_functions")
        for function in functions:
            name_lower = function['name'].lower()
            self.existing_functions[name_lower] = {
                'id': function['id'],
                'name': function['name']
            }
        logger.info(f"  Loaded {len(self.existing_functions)} existing business functions")

        # Load existing smartjects
        smartjects = self.supabase.fetch_all_smartjects()
        for smartject in smartjects:
            title_lower = smartject['title'].lower()
            self.existing_smartjects[title_lower] = smartject['id']
        logger.info(f"  Loaded {len(self.existing_smartjects)} existing smartjects")

    def validate_and_parse_audience(self, value: str) -> Tuple[bool, List[str], str]:
        """
        Validate and parse audience field which can be:
        1. JSON array: ["Audience1", "Audience2"]
        2. Comma-separated: "Audience1, Audience2, and Audience3"
        Returns (is_valid, parsed_list, format_type)
        """
        if pd.isna(value) or not str(value).strip():
            return True, [], "empty"

        value_str = str(value).strip()

        # First try to parse as JSON array
        if value_str.startswith('[') and value_str.endswith(']'):
            try:
                parsed = json.loads(value_str)

                if not isinstance(parsed, list):
                    logger.debug(f"    Audience parsed but not a list: {value_str[:100]}")
                    return False, [], "invalid_json"

                # Validate all items are strings
                valid_items = []
                for item in parsed:
                    if isinstance(item, str) and item.strip():
                        valid_items.append(item.strip())
                    else:
                        logger.debug(f"    Audience has invalid item: {item}")
                        return False, [], "invalid_json_item"

                return True, valid_items, "json_array"

            except json.JSONDecodeError as e:
                logger.debug(f"    Audience JSON decode error: {e}")
                return False, [], "json_error"

        # If not JSON array, try to parse as comma-separated
        else:
            # Parse comma-separated format
            items = self.parse_comma_separated_audience(value_str)

            if items:
                logger.debug(f"    Parsed comma-separated audience: {len(items)} items")
                return True, items, "comma_separated"
            else:
                return False, [], "invalid_format"

    def validate_json_array(self, value: str, field_name: str) -> Tuple[bool, List[str]]:
        """
        Validate that a field contains a valid JSON array of strings.
        For non-audience fields, only accepts JSON format.
        Returns (is_valid, parsed_list)
        """
        if pd.isna(value) or not str(value).strip():
            return True, []  # Empty is valid

        value_str = str(value).strip()

        # Must be a JSON array
        if not (value_str.startswith('[') and value_str.endswith(']')):
            logger.debug(f"    {field_name} not a JSON array: {value_str[:100]}")
            return False, []

        try:
            parsed = json.loads(value_str)

            if not isinstance(parsed, list):
                logger.debug(f"    {field_name} parsed but not a list: {value_str[:100]}")
                return False, []

            # Validate all items are strings
            valid_items = []
            for item in parsed:
                if isinstance(item, str) and item.strip():
                    valid_items.append(item.strip())
                else:
                    logger.debug(f"    {field_name} has invalid item: {item}")
                    return False, []

            return True, valid_items

        except json.JSONDecodeError as e:
            logger.debug(f"    {field_name} JSON decode error: {e}")
            return False, []

    def get_or_create_reference_item(self, name: str, ref_type: str) -> Optional[Dict]:
        """Get or create a reference item (audience, industry, or business_function)"""
        if not name or not name.strip():
            return None

        name = name.strip()
        name_lower = name.lower()

        # Select appropriate cache, create method, and table name
        if ref_type == 'audience':
            cache = self.existing_audiences
            create_method = self.supabase.insert_audience
            stat_key = 'new_audiences'
            table_name = 'audience'
        elif ref_type == 'industry':
            cache = self.existing_industries
            create_method = self.supabase.insert_industry
            stat_key = 'new_industries'
            table_name = 'industries'
        elif ref_type == 'function':
            cache = self.existing_functions
            create_method = self.supabase.insert_business_function
            stat_key = 'new_functions'
            table_name = 'business_functions'
        else:
            logger.error(f"Invalid reference type: {ref_type}")
            return None

        # Check if already exists in local cache
        if name_lower in cache:
            logger.debug(f"    Using cached {ref_type}: {name}")
            return cache[name_lower]

        # Check if exists in database before trying to create
        try:
            existing = self.supabase.client.table(table_name).select("*").ilike("name", name).execute()
            was_found_in_db = bool(existing.data)
        except:
            was_found_in_db = False

        # Get or create item
        logger.debug(f"    Getting or creating {ref_type}: {name}")
        item = create_method(name)

        if item:
            # Update cache
            cache[name_lower] = {
                'id': item['id'],
                'name': item['name']
            }

            # Only increment stats if it was actually created (not found)
            if not was_found_in_db:
                logger.info(f"    Created new {ref_type}: {name}")
                self.stats[stat_key] += 1
            else:
                logger.info(f"    Found existing {ref_type}: {name}")

            return item

        logger.error(f"    Failed to get/create {ref_type}: {name}")
        return None

    def process_smartject_row(self, row: pd.Series, row_idx: int, dry_run: bool = False) -> Dict:
        """Process a single row from the XLSX file"""
        result = {
            'row': row_idx,
            'title': None,
            'status': 'unknown',
            'message': '',
            'audiences': [],
            'industries': [],
            'functions': []
        }

        # Extract title
        title = str(row.get('name', '')).strip() if not pd.isna(row.get('name')) else ''
        if not title:
            result['status'] = 'skipped'
            result['message'] = 'No title'
            self.stats['skipped_smartjects'] += 1
            return result

        result['title'] = title
        logger.info(f"\n[Row {row_idx}] Processing: {title}")

        # Parse audience format (accepts both JSON array and comma-separated)
        audience_value = row.get('audience', '')
        is_valid_audience, audience_list, format_type = self.validate_and_parse_audience(audience_value)

        if not is_valid_audience:
            logger.warning(f"  ✕ Invalid audience format for '{title}'")
            logger.debug(f"    Raw value: {audience_value}")
            result['status'] = 'invalid_audience'
            result['message'] = f'Invalid audience format ({format_type})'
            self.stats['invalid_audience_format'] += 1
            self.stats['skipped_smartjects'] += 1
            return result

        if audience_list and format_type == 'comma_separated':
            logger.info(f"  ✓ Parsed comma-separated audience: {len(audience_list)} items")
            logger.debug(f"    Items: {audience_list}")

        # Parse other array fields (industries and functions)
        is_valid_industries, industries_list = self.validate_json_array(
            row.get('industries', ''), 'industries'
        )
        is_valid_functions, functions_list = self.validate_json_array(
            row.get('functions', ''), 'functions'
        )

        # For industries and functions, we're more lenient - just log warnings
        if not is_valid_industries:
            logger.warning(f"  ⚠ Invalid industries format for '{title}', using empty list")
            industries_list = []

        if not is_valid_functions:
            logger.warning(f"  ⚠ Invalid functions format for '{title}', using empty list")
            functions_list = []

        result['audiences'] = audience_list
        result['industries'] = industries_list
        result['functions'] = functions_list

        # Check if smartject already exists
        title_lower = title.lower()
        existing_id = self.existing_smartjects.get(title_lower)

        if dry_run:
            if existing_id:
                result['status'] = 'would_update'
                result['message'] = f'Would update existing smartject (ID: {existing_id})'
            else:
                result['status'] = 'would_create'
                result['message'] = 'Would create new smartject'
            return result

        # Prepare smartject data
        smartject_data = {
            'title': title,
            'mission': str(row.get('mission', '')).strip() if not pd.isna(row.get('mission')) else '',
            'problematics': str(row.get('problematics', '')).strip() if not pd.isna(row.get('problematics')) else '',
            'scope': str(row.get('scope', '')).strip() if not pd.isna(row.get('scope')) else '',
            'audience': json.dumps(audience_list),  # Store as JSON string
            'how_it_works': str(row.get('how it works', '')).strip() if not pd.isna(row.get('how it works')) else '',
            'architecture': str(row.get('architecture', '')).strip() if not pd.isna(row.get('architecture')) else '',
            'innovation': str(row.get('innovation', '')).strip() if not pd.isna(row.get('innovation')) else '',
            'use_case': str(row.get('use case', '')).strip() if not pd.isna(row.get('use case')) else '',
            'updated_at': datetime.now().isoformat()
        }

        try:
            if existing_id:
                # Update existing smartject
                logger.info(f"  Updating existing smartject (ID: {existing_id})")

                # Update main smartject data
                self.supabase.client.table("smartjects").update(
                    smartject_data
                ).eq('id', existing_id).execute()

                smartject_id = existing_id
                self.stats['updated_smartjects'] += 1
                result['status'] = 'updated'

            else:
                # Create new smartject
                logger.info(f"  Creating new smartject")

                smartject_id = str(uuid.uuid4())
                smartject_data['id'] = smartject_id
                smartject_data['created_at'] = datetime.now().isoformat()

                # Handle team field
                team_value = row.get('team', '')
                is_valid_team, team_list = self.validate_json_array(team_value, 'team')
                if is_valid_team:
                    smartject_data['team'] = team_list
                else:
                    smartject_data['team'] = []

                # Insert smartject
                self.supabase.client.table("smartjects").insert(smartject_data).execute()

                # Add to cache
                self.existing_smartjects[title_lower] = smartject_id
                self.stats['created_smartjects'] += 1
                result['status'] = 'created'

            # Process audiences
            if audience_list:
                logger.info(f"  Processing {len(audience_list)} audiences")
                audience_ids = []

                for audience_name in audience_list:
                    audience = self.get_or_create_reference_item(audience_name, 'audience')
                    if audience:
                        audience_ids.append(audience['id'])

                # Clear existing relations and create new ones
                if audience_ids:
                    # Delete existing relations
                    self.supabase.client.table("smartject_audience").delete().eq(
                        "smartject_id", smartject_id
                    ).execute()

                    # Create new relations
                    relations = [
                        {'smartject_id': smartject_id, 'audience_id': aid}
                        for aid in audience_ids
                    ]
                    self.supabase.client.table("smartject_audience").insert(relations).execute()
                    logger.info(f"    Created {len(relations)} audience relations")

            # Process industries
            if industries_list:
                logger.info(f"  Processing {len(industries_list)} industries")
                industry_ids = []

                for industry_name in industries_list:
                    industry = self.get_or_create_reference_item(industry_name, 'industry')
                    if industry:
                        industry_ids.append(industry['id'])

                if industry_ids:
                    # Delete existing relations
                    self.supabase.client.table("smartject_industries").delete().eq(
                        "smartject_id", smartject_id
                    ).execute()

                    # Create new relations
                    relations = [
                        {'smartject_id': smartject_id, 'industry_id': iid}
                        for iid in industry_ids
                    ]
                    self.supabase.client.table("smartject_industries").insert(relations).execute()
                    logger.info(f"    Created {len(relations)} industry relations")

            # Process business functions
            if functions_list:
                logger.info(f"  Processing {len(functions_list)} business functions")
                function_ids = []

                for function_name in functions_list:
                    function = self.get_or_create_reference_item(function_name, 'function')
                    if function:
                        function_ids.append(function['id'])

                if function_ids:
                    # Delete existing relations
                    self.supabase.client.table("smartject_business_functions").delete().eq(
                        "smartject_id", smartject_id
                    ).execute()

                    # Create new relations
                    relations = [
                        {'smartject_id': smartject_id, 'function_id': fid}
                        for fid in function_ids
                    ]
                    self.supabase.client.table("smartject_business_functions").insert(relations).execute()
                    logger.info(f"    Created {len(relations)} function relations")

            self.stats['valid_smartjects'] += 1
            result['message'] = f'Successfully {result["status"]} smartject'

        except Exception as e:
            logger.error(f"  Error processing smartject: {e}")
            result['status'] = 'error'
            result['message'] = str(e)
            self.stats['errors'].append({
                'row': row_idx,
                'title': title,
                'error': str(e)
            })

        return result

    def process_xlsx_file(self, file_path: str, sheet_name: str = 'smartjects',
                         limit: Optional[int] = None, dry_run: bool = False) -> List[Dict]:
        """Process XLSX file and sync smartjects"""

        logger.info("=" * 60)
        logger.info(f"Processing XLSX file: {file_path}")
        logger.info(f"Sheet: {sheet_name}")
        logger.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
        logger.info("=" * 60)

        # Load existing data
        self.load_existing_data()

        # Read XLSX file
        try:
            logger.info(f"\nReading XLSX file...")
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            self.stats['total_rows'] = len(df)
            logger.info(f"Found {len(df)} rows in sheet '{sheet_name}'")

            # Apply limit if specified
            if limit:
                df = df.head(limit)
                logger.info(f"Processing first {limit} rows")

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error reading XLSX file: {e}")
            sys.exit(1)

        # Process each row
        results = []
        for idx, row in df.iterrows():
            result = self.process_smartject_row(row, idx + 2, dry_run=dry_run)  # +2 for Excel row number
            results.append(result)

            # Progress update
            if (idx + 1) % 10 == 0:
                logger.info(f"\nProgress: {idx + 1}/{len(df)} rows processed")

        return results

    def print_summary(self, results: List[Dict], dry_run: bool = False):
        """Print processing summary"""
        logger.info("\n" + "=" * 60)
        logger.info("PROCESSING SUMMARY" + (" [DRY RUN]" if dry_run else ""))
        logger.info("=" * 60)

        logger.info(f"Total rows processed: {self.stats['total_rows']}")
        logger.info(f"  ✓ Valid smartjects: {self.stats['valid_smartjects']}")

        if not dry_run:
            logger.info(f"    - Created: {self.stats['created_smartjects']}")
            logger.info(f"    - Updated: {self.stats['updated_smartjects']}")
        else:
            would_create = len([r for r in results if r['status'] == 'would_create'])
            would_update = len([r for r in results if r['status'] == 'would_update'])
            logger.info(f"    - Would create: {would_create}")
            logger.info(f"    - Would update: {would_update}")

        logger.info(f"  ✕ Invalid audience format: {self.stats['invalid_audience_format']}")
        logger.info(f"  ⚠ Skipped: {self.stats['skipped_smartjects']}")

        if self.stats['new_audiences'] > 0:
            logger.info(f"\nNew reference items created:")
            logger.info(f"  - Audiences: {self.stats['new_audiences']}")
            logger.info(f"  - Industries: {self.stats['new_industries']}")
            logger.info(f"  - Functions: {self.stats['new_functions']}")

        # Show invalid audience format smartjects
        invalid = [r for r in results if r['status'] == 'invalid_audience']
        if invalid:
            logger.info("\n" + "=" * 60)
            logger.info("SMARTJECTS WITH INVALID AUDIENCE FORMAT")
            logger.info("=" * 60)
            logger.info("These smartjects were skipped due to invalid audience format.")
            logger.info("Accepted formats:")
            logger.info('  1. JSON array: ["Audience1", "Audience2", ...]')
            logger.info('  2. Comma-separated: "Audience1, Audience2, and Audience3"')
            logger.info("")

            for item in invalid[:10]:
                logger.warning(f"  Row {item['row']}: {item['title']}")

            if len(invalid) > 10:
                logger.warning(f"  ... and {len(invalid) - 10} more")

        # Show errors
        if self.stats['errors']:
            logger.info("\n" + "=" * 60)
            logger.info("ERRORS")
            logger.info("=" * 60)

            for error in self.stats['errors'][:10]:
                logger.error(f"  Row {error['row']}: {error['title']}")
                logger.error(f"    Error: {error['error']}")

            if len(self.stats['errors']) > 10:
                logger.error(f"  ... and {len(self.stats['errors']) - 10} more errors")


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Sync smartjects from XLSX file with strict audience validation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Accepted audience formats:
  1. JSON array: ["AI researchers", "Data scientists", "Engineers"]
  2. Comma-separated: "AI researchers, data scientists, and engineers"

Examples:
  # Process all rows (dry run)
  python sync_from_xlsx.py smartjects.xlsx

  # Process and update database
  python sync_from_xlsx.py smartjects.xlsx --no-dry-run

  # Process first 10 rows only
  python sync_from_xlsx.py smartjects.xlsx --limit 10

  # Use different sheet name
  python sync_from_xlsx.py smartjects.xlsx --sheet data
        """
    )

    parser.add_argument('file', help='Path to XLSX file')
    parser.add_argument('--sheet', default='smartjects',
                       help='Sheet name to process (default: smartjects)')
    parser.add_argument('--limit', type=int,
                       help='Process only first N rows')
    parser.add_argument('--no-dry-run', action='store_true',
                       help='Actually update the database (default is dry run)')

    args = parser.parse_args()

    # Check if file exists
    if not Path(args.file).exists():
        logger.error(f"File not found: {args.file}")
        sys.exit(1)

    synchronizer = XLSXSmartjectSynchronizer()

    try:
        dry_run = not args.no_dry_run

        if dry_run:
            logger.info("🔍 Running in DRY RUN mode - no changes will be made")
            logger.info("   Use --no-dry-run to actually update the database\n")
        else:
            logger.warning("⚠️  Running in LIVE mode - database WILL be updated!")
            response = input("Are you sure you want to continue? (yes/no): ")
            if response.lower() != 'yes':
                logger.info("Aborted by user")
                sys.exit(0)

        # Process the file
        results = synchronizer.process_xlsx_file(
            args.file,
            sheet_name=args.sheet,
            limit=args.limit,
            dry_run=dry_run
        )

        # Print summary
        synchronizer.print_summary(results, dry_run=dry_run)

        # Final status
        logger.info("\n" + "=" * 60)
        if dry_run:
            logger.info("✅ Dry run completed! Use --no-dry-run to apply changes.")
        else:
            logger.info("✅ Synchronization completed!")
        logger.info("=" * 60)

        # Exit with error code if there were invalid formats
        if synchronizer.stats['invalid_audience_format'] > 0:
            sys.exit(1)
        else:
            sys.exit(0)

    except KeyboardInterrupt:
        logger.info("\n\nOperation interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
