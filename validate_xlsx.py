#!/usr/bin/env python3
"""
Script to validate audience format in XLSX file without modifying the database.
This script:
1. Reads smartjects from XLSX file
2. Validates that audience field is in strict JSON array format
3. Reports which rows have invalid format
4. Shows statistics and examples of invalid formats
"""

import os
import sys
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from collections import defaultdict

# Add the bot directory to Python path
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / 'bot'))

from bot.utils.logging_config import get_logger

logger = get_logger(__name__)


class XLSXValidator:
    """Validates XLSX file format, focusing on audience field"""

    def __init__(self):
        self.stats = {
            'total_rows': 0,
            'valid_rows': 0,
            'invalid_audience': 0,
            'empty_audience': 0,
            'missing_title': 0,
            'invalid_industries': 0,
            'invalid_functions': 0
        }
        self.invalid_rows = []
        self.format_examples = defaultdict(list)  # Format type -> list of examples

    def parse_comma_separated(self, text: str) -> List[str]:
        """Parse comma-separated text into list"""
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

    def validate_audience_field(self, value: str) -> Tuple[bool, List[str], str]:
        """
        Validate audience field which can be:
        1. JSON array: ["Audience1", "Audience2"]
        2. Comma-separated: "Audience1, Audience2, and Audience3"
        Returns (is_valid, parsed_list, format_description)
        """
        if pd.isna(value) or not str(value).strip():
            return True, [], "empty"

        value_str = str(value).strip()

        # First try JSON array
        if value_str.startswith('[') and value_str.endswith(']'):
            try:
                parsed = json.loads(value_str)

                if not isinstance(parsed, list):
                    return False, [], "invalid_json_not_list"

                # Validate all items are strings
                valid_items = []
                for item in parsed:
                    if isinstance(item, str) and item.strip():
                        valid_items.append(item.strip())
                    else:
                        return False, [], "invalid_json_item"

                return True, valid_items, "json_array"

            except json.JSONDecodeError as e:
                return False, [], f"json_error: {str(e)[:50]}"

        # Try comma-separated format
        else:
            items = self.parse_comma_separated(value_str)
            if items:
                return True, items, "comma_separated"
            else:
                return False, [], "invalid_format"

    def validate_json_array(self, value: str, field_name: str) -> Tuple[bool, List[str], str]:
        """
        Validate that a field contains a valid JSON array of strings.
        For non-audience fields, only accepts JSON format.
        Returns (is_valid, parsed_list, error_message)
        """
        if pd.isna(value) or not str(value).strip():
            return True, [], "empty"

        value_str = str(value).strip()

        # Check if it looks like a JSON array
        if not (value_str.startswith('[') and value_str.endswith(']')):
            return False, [], "not_json_array"

        try:
            parsed = json.loads(value_str)

            if not isinstance(parsed, list):
                return False, [], "not_a_list"

            # Validate all items are strings
            valid_items = []
            for i, item in enumerate(parsed):
                if not isinstance(item, str):
                    return False, [], f"item_{i}_not_string"
                if not item.strip():
                    return False, [], f"item_{i}_empty"
                valid_items.append(item.strip())

            return True, valid_items, "valid"

        except json.JSONDecodeError as e:
            return False, [], f"json_error: {str(e)[:50]}"

    def detect_format_type(self, value: str) -> str:
        """Detect what format the value is in"""
        if pd.isna(value) or not str(value).strip():
            return "empty"

        value_str = str(value).strip()

        # Check for Python list format ['item1', 'item2']
        if value_str.startswith('[') and value_str.endswith(']'):
            if "'" in value_str:
                return "python_list"
            elif '"' in value_str:
                return "json_array"
            else:
                return "malformed_array"

        # Check for comma-separated
        if ',' in value_str and not value_str.startswith('['):
            return "comma_separated"

        # Check for semicolon-separated
        if ';' in value_str and not value_str.startswith('['):
            return "semicolon_separated"

        # Single value
        if not any(char in value_str for char in [',', ';', '[', ']']):
            return "single_value"

        return "unknown_format"

    def validate_row(self, row: pd.Series, row_idx: int) -> Dict:
        """Validate a single row"""
        result = {
            'row': row_idx,
            'title': None,
            'valid': True,
            'errors': [],
            'warnings': []
        }

        # Check title
        title = str(row.get('name', '')).strip() if not pd.isna(row.get('name')) else ''
        if not title:
            result['errors'].append("Missing title (name field)")
            result['valid'] = False
            self.stats['missing_title'] += 1
        else:
            result['title'] = title

        # Validate audience (accepts JSON array or comma-separated)
        audience_value = row.get('audience', '')
        is_valid, audience_list, format_msg = self.validate_audience_field(audience_value)

        if not is_valid:
            if format_msg == "empty":
                result['warnings'].append("Empty audience field")
                self.stats['empty_audience'] += 1
            else:
                result['errors'].append(f"Invalid audience format: {format_msg}")
                result['valid'] = False
                self.stats['invalid_audience'] += 1

                # Detect format type for reporting
                format_type = self.detect_format_type(audience_value)
                self.format_examples[format_type].append({
                    'row': row_idx,
                    'title': title,
                    'value': str(audience_value)[:200]
                })
        else:
            if audience_list:
                result['audience_count'] = len(audience_list)
                result['audience_sample'] = audience_list[:3]
                if format_msg == "comma_separated":
                    result['audience_format'] = "comma_separated"

        # Validate industries (optional but should be valid if present)
        industries_value = row.get('industries', '')
        is_valid, industries_list, error_msg = self.validate_json_array(industries_value, 'industries')

        if not is_valid and error_msg != "empty":
            result['warnings'].append(f"Invalid industries format: {error_msg}")
            self.stats['invalid_industries'] += 1

        # Validate functions (optional but should be valid if present)
        functions_value = row.get('functions', '')
        is_valid, functions_list, error_msg = self.validate_json_array(functions_value, 'functions')

        if not is_valid and error_msg != "empty":
            result['warnings'].append(f"Invalid functions format: {error_msg}")
            self.stats['invalid_functions'] += 1

        return result

    def validate_xlsx(self, file_path: str, sheet_name: str = 'smartjects',
                     limit: Optional[int] = None) -> List[Dict]:
        """Validate XLSX file"""

        logger.info("=" * 60)
        logger.info(f"Validating XLSX file: {file_path}")
        logger.info(f"Sheet: {sheet_name}")
        logger.info("=" * 60)

        # Read XLSX file
        try:
            logger.info(f"\nReading XLSX file...")
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            self.stats['total_rows'] = len(df)
            logger.info(f"Found {len(df)} rows in sheet '{sheet_name}'")

            if limit:
                df = df.head(limit)
                logger.info(f"Validating first {limit} rows")

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error reading XLSX file: {e}")
            sys.exit(1)

        # Validate each row
        results = []
        for idx, row in df.iterrows():
            result = self.validate_row(row, idx + 2)  # +2 for Excel row number (1-based + header)
            results.append(result)

            if result['valid']:
                self.stats['valid_rows'] += 1
            else:
                self.invalid_rows.append(result)

            # Progress update
            if (idx + 1) % 50 == 0:
                logger.info(f"Progress: {idx + 1}/{len(df)} rows validated")

        return results

    def print_report(self, results: List[Dict]):
        """Print validation report"""

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("VALIDATION SUMMARY")
        logger.info("=" * 60)

        logger.info(f"Total rows: {self.stats['total_rows']}")
        logger.info(f"  ✓ Valid rows: {self.stats['valid_rows']}")
        logger.info(f"  ✗ Invalid rows: {len(self.invalid_rows)}")

        if self.stats['missing_title'] > 0:
            logger.info(f"\nMissing titles: {self.stats['missing_title']}")

        logger.info(f"\nAudience field issues:")
        logger.info(f"  ✗ Invalid format: {self.stats['invalid_audience']}")
        logger.info(f"  ⚠ Empty: {self.stats['empty_audience']}")

        if self.stats['invalid_industries'] > 0 or self.stats['invalid_functions'] > 0:
            logger.info(f"\nOther field issues (warnings):")
            logger.info(f"  ⚠ Invalid industries format: {self.stats['invalid_industries']}")
            logger.info(f"  ⚠ Invalid functions format: {self.stats['invalid_functions']}")

        # Format analysis
        if self.format_examples:
            logger.info("\n" + "=" * 60)
            logger.info("AUDIENCE FORMAT ANALYSIS")
            logger.info("=" * 60)
            logger.info("Found the following invalid formats:")

            for format_type, examples in self.format_examples.items():
                logger.info(f"\n{format_type.replace('_', ' ').title()}: {len(examples)} occurrences")

                # Show first 3 examples
                for example in examples[:3]:
                    logger.info(f"  Row {example['row']}: {example['title']}")
                    logger.info(f"    Value: {example['value']}")

                if len(examples) > 3:
                    logger.info(f"  ... and {len(examples) - 3} more")

        # Invalid rows details
        if self.invalid_rows:
            logger.info("\n" + "=" * 60)
            logger.info("INVALID ROWS (First 20)")
            logger.info("=" * 60)
            logger.info("Accepted audience formats:")
            logger.info('  1. JSON array: ["Audience1", "Audience2", ...]')
            logger.info('  2. Comma-separated: "Audience1, Audience2, and Audience3"')
            logger.info("")

            for row in self.invalid_rows[:20]:
                logger.error(f"Row {row['row']}: {row['title'] or 'NO TITLE'}")
                for error in row['errors']:
                    logger.error(f"  ✗ {error}")
                for warning in row.get('warnings', []):
                    logger.warning(f"  ⚠ {warning}")

            if len(self.invalid_rows) > 20:
                logger.error(f"\n... and {len(self.invalid_rows) - 20} more invalid rows")

        # Valid examples
        valid_with_audience = [r for r in results if r['valid'] and r.get('audience_count', 0) > 0]
        if valid_with_audience:
            logger.info("\n" + "=" * 60)
            logger.info("VALID FORMAT EXAMPLES")
            logger.info("=" * 60)

            for row in valid_with_audience[:5]:
                logger.info(f"Row {row['row']}: {row['title']}")
                logger.info(f"  Audiences ({row['audience_count']}): {', '.join(row['audience_sample'])}")
                if row['audience_count'] > 3:
                    logger.info(f"  ... and {row['audience_count'] - 3} more")

    def export_report(self, results: List[Dict], output_file: str = "validation_report.json"):
        """Export validation report to JSON"""
        import json

        report = {
            'summary': self.stats,
            'format_analysis': {
                format_type: len(examples)
                for format_type, examples in self.format_examples.items()
            },
            'invalid_rows': self.invalid_rows,
            'format_examples': dict(self.format_examples)
        }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        logger.info(f"\nDetailed report exported to: {output_file}")


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Validate audience format in XLSX file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script validates that the audience field in the XLSX file is in an accepted format.

Accepted formats:
  1. JSON array: ["AI researchers", "Data scientists", "Engineers"]
  2. Comma-separated: "AI researchers, data scientists, and engineers"

Examples:
  # Validate all rows
  python validate_xlsx.py smartjects.xlsx

  # Validate first 100 rows
  python validate_xlsx.py smartjects.xlsx --limit 100

  # Use different sheet name
  python validate_xlsx.py smartjects.xlsx --sheet data

  # Export detailed report
  python validate_xlsx.py smartjects.xlsx --export-report
        """
    )

    parser.add_argument('file', help='Path to XLSX file')
    parser.add_argument('--sheet', default='smartjects',
                       help='Sheet name to validate (default: smartjects)')
    parser.add_argument('--limit', type=int,
                       help='Validate only first N rows')
    parser.add_argument('--export-report', action='store_true',
                       help='Export detailed report to JSON file')
    parser.add_argument('--report-file', default='validation_report.json',
                       help='Report filename (default: validation_report.json)')

    args = parser.parse_args()

    # Check if file exists
    if not Path(args.file).exists():
        logger.error(f"File not found: {args.file}")
        sys.exit(1)

    validator = XLSXValidator()

    try:
        # Validate the file
        results = validator.validate_xlsx(
            args.file,
            sheet_name=args.sheet,
            limit=args.limit
        )

        # Print report
        validator.print_report(results)

        # Export if requested
        if args.export_report:
            validator.export_report(results, args.report_file)

        # Final status
        logger.info("\n" + "=" * 60)
        if validator.stats['invalid_audience'] == 0:
            logger.info("✅ All audience formats are valid!")
        else:
            logger.warning(f"⚠️  Found {validator.stats['invalid_audience']} rows with invalid audience format")
            logger.info("Fix these formats to: [\"Audience1\", \"Audience2\", ...]")
        logger.info("=" * 60)

        # Exit with error code if there were invalid formats
        sys.exit(1 if validator.stats['invalid_audience'] > 0 else 0)

    except KeyboardInterrupt:
        logger.info("\n\nValidation interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
