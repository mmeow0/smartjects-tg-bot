import csv
import uuid

import ast
import json
import re
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
from io import StringIO
import asyncio
from difflib import SequenceMatcher

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
        self.refresh_reference_data()

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
            'with_logos': 0,
            'unmapped_industries': 0,
            'unmapped_audience': 0,
            'unmapped_functions': 0,
            'total_unmapped': 0
        }

        # Track unmapped tags for review
        self.unmapped_tags = {
            'industries': [],
            'audience': [],
            'functions': []
        }

    def refresh_reference_data(self):
        """Refresh reference data from database to get latest entries"""
        logger.info("Refreshing reference data from database...")

        # Fetch reference data
        self.industries = self.supabase.fetch_reference_table("industries")
        self.audience = self.supabase.fetch_reference_table("audience")
        self.business_functions = self.supabase.fetch_reference_table("business_functions")

        # Create lookup dictionaries
        self.industries_dict = {item["name"].lower(): item for item in self.industries}
        self.audience_dict = {item["name"].lower(): item for item in self.audience}
        self.functions_dict = {item["name"].lower(): item for item in self.business_functions}

        logger.info(f"Loaded {len(self.industries)} industries, {len(self.audience)} audiences, {len(self.business_functions)} business functions")

    def parse_csv_array(self, value: str, strict_json: bool = False) -> List[str]:
        """Parse CSV array string to Python list

        Args:
            value: String value to parse
            strict_json: If True, only accept valid JSON arrays
        """
        if not value or value.strip() == '':
            return []

        value = value.strip()

        # If strict JSON mode, only accept proper JSON arrays
        if strict_json:
            if not (value.startswith('[') and value.endswith(']')):
                return []

            try:
                import json
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    # Validate all items are strings
                    valid_items = []
                    for item in parsed:
                        if isinstance(item, str) and item.strip():
                            valid_items.append(item.strip())
                        else:
                            logger.warning(f"Invalid item in JSON array: {item}")
                            return []  # Invalid format, reject entire array
                    return valid_items
                else:
                    logger.warning(f"Value is not a JSON array: {value}")
                    return []
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse as JSON array: {value}, error: {e}")
                return []

        # Original parsing logic for non-strict mode
        try:
            # Try to parse as Python list
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed]
        except:
            pass

        # If that fails, split by commas
        return [item.strip().strip('"').strip("'") for item in value.split(',') if item.strip()]

    def calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity between two text strings"""
        return SequenceMatcher(None, text1.lower(), text2.lower()).ratio()

    def find_best_match(self, tag: str, reference_dict: Dict[str, Dict],
                       category_type: str = None) -> Optional[str]:
        """Find best matching entry from existing database entries"""
        tag_lower = tag.lower()
        tag_words = set(tag_lower.split())

        # First try exact match
        if tag_lower in reference_dict:
            return reference_dict[tag_lower]["name"]

        # Try partial word matching
        for name_lower, item in reference_dict.items():
            name_words = set(name_lower.split())
            # Check if all tag words are in the name
            if tag_words.issubset(name_words):
                return item["name"]
            # Check if all name words are in the tag
            if name_words.issubset(tag_words):
                return item["name"]

        # Try to find best match using appropriate logic for each type
        if category_type == "function":
            return self.find_best_function_match(tag, reference_dict)
        elif category_type == "industry":
            return self.find_best_industry_match(tag, reference_dict)
        elif category_type == "audience":
            return self.find_best_audience_match(tag, reference_dict)

        # Fallback to similarity matching with partial matches
        best_match = None
        best_score = 0.5  # Lower threshold for better matching

        for name_lower, item in reference_dict.items():
            # Calculate base similarity
            similarity = self.calculate_similarity(tag_lower, name_lower)

            # Bonus for partial word matches
            name_words = set(name_lower.split())
            common_words = tag_words & name_words
            if common_words:
                word_bonus = len(common_words) / max(len(tag_words), len(name_words)) * 0.3
                similarity += word_bonus

            if similarity > best_score:
                best_score = similarity
                best_match = item["name"]

        return best_match

    def find_best_function_match(self, tag: str, reference_dict: Dict[str, Dict]) -> Optional[str]:
        """Find best matching business function using categorization logic"""
        tag_lower = tag.lower()

        # Define keyword patterns and synonyms for business functions
        function_keywords = {
            "data": ["Data & Analytics", "analytics", "analysis", "visualization", "database", "bi", "insight", "metrics"],
            "ai": ["AI & Machine Learning", "artificial intelligence", "machine learning", "ml", "deep learning", "neural", "nlp"],
            "software": ["Software Development", "development", "programming", "application", "coding", "dev", "app", "system"],
            "customer": ["Customer & User Experience", "user", "experience", "ux", "support", "service", "client", "cx"],
            "marketing": ["Marketing & Sales", "sales", "advertising", "brand", "campaign", "promotion", "growth", "lead"],
            "operations": ["Operations & Management", "management", "process", "workflow", "efficiency", "optimization"],
            "finance": ["Finance & Accounting", "accounting", "payment", "budget", "investment", "financial", "banking"],
            "hr": ["Human Resources", "employee", "talent", "recruitment", "training", "people", "workforce", "hiring"],
            "security": ["Security & Compliance", "cyber", "privacy", "protection", "compliance", "risk", "audit"],
            "communication": ["Communication & Collaboration", "collaboration", "messaging", "team", "chat", "meeting"],
            "healthcare": ["Healthcare & Medical", "medical", "clinical", "patient", "therapy", "health", "hospital"],
            "education": ["Education & Training", "training", "learning", "teaching", "course", "e-learning", "educational"],
            "supply": ["Supply Chain & Logistics", "logistics", "inventory", "shipping", "distribution", "warehouse"],
            "manufacturing": ["Manufacturing & Production", "production", "industrial", "factory", "assembly", "quality"],
            "media": ["Media & Content", "content", "video", "audio", "publishing", "streaming", "broadcast"],
            "research": ["Research & Development", "r&d", "innovation", "experiment", "laboratory", "testing"],
            "legal": ["Legal & Regulatory", "law", "regulatory", "compliance", "contract", "governance", "policy"],
            "environmental": ["Environmental & Sustainability", "sustainability", "green", "renewable", "eco", "climate"],
            "real estate": ["Real Estate & Property", "property", "building", "facility", "construction", "architecture"]
        }

        # Synonyms mapping
        synonyms = {
            "it": "software", "tech": "software", "technology": "software",
            "ml": "ai", "artificial intelligence": "ai", "machine learning": "ai",
            "crm": "customer", "ux": "customer", "ui": "customer", "cx": "customer",
            "hr": "human resources", "people": "hr", "talent": "hr",
            "cyber": "security", "cybersecurity": "security", "infosec": "security",
            "collab": "communication", "comms": "communication",
            "med": "healthcare", "medical": "healthcare", "pharma": "healthcare",
            "edu": "education", "training": "education", "learning": "education",
            "scm": "supply", "logistics": "supply", "shipping": "supply",
            "mfg": "manufacturing", "production": "manufacturing",
            "r&d": "research", "rd": "research", "innovation": "research"
        }

        # Normalize tag using synonyms
        tag_normalized = tag_lower
        for syn, replacement in synonyms.items():
            if syn in tag_normalized:
                tag_normalized = tag_normalized.replace(syn, replacement)

        best_match = None
        best_score = 0

        # Check each existing function for keyword matches
        for name_lower, item in reference_dict.items():
            score = 0

            # Check for keyword matches with enhanced scoring
            for keyword_group, keywords in function_keywords.items():
                for keyword in keywords:
                    if keyword in tag_normalized and keyword in name_lower:
                        score += len(keyword) * 3  # Increased weight
                    elif keyword in tag_normalized and keyword_group in name_lower:
                        score += len(keyword) * 2
                    # Partial word matching
                    elif any(word in tag_normalized for word in keyword.split()):
                        score += 1

            # Enhanced similarity calculation
            similarity = self.calculate_similarity(tag_normalized, name_lower)

            # Check for word overlap
            tag_words = set(tag_normalized.split())
            name_words = set(name_lower.split())
            common_words = tag_words & name_words
            if common_words:
                word_score = len(common_words) / min(len(tag_words), len(name_words)) * 5
                score += word_score

            score += similarity * 10

            if score > best_score:
                best_score = score
                best_match = item["name"]

        return best_match if best_score > 3 else None  # Lower threshold for better matching

    def find_best_industry_match(self, tag: str, reference_dict: Dict[str, Dict]) -> Optional[str]:
        """Find best matching industry using categorization logic"""
        tag_lower = tag.lower()

        # Enhanced industry keyword patterns with synonyms
        industry_keywords = {
            "healthcare": ["health", "medical", "clinical", "hospital", "pharma", "medicine", "care", "patient", "therapy", "nursing"],
            "technology": ["software", "tech", "IT", "computer", "digital", "ai", "app", "platform", "system", "cloud", "data"],
            "finance": ["finance", "bank", "investment", "trading", "insurance", "fintech", "financial", "banking", "fund"],
            "education": ["education", "training", "learning", "academic", "university", "school", "college", "teaching", "edu"],
            "manufacturing": ["manufacturing", "industrial", "factory", "production", "assembly", "machinery", "automotive"],
            "energy": ["energy", "power", "renewable", "solar", "oil", "gas", "utilities", "electricity", "wind", "nuclear"],
            "transportation": ["transport", "logistics", "shipping", "delivery", "aviation", "airline", "freight", "supply chain"],
            "retail": ["retail", "e-commerce", "shopping", "store", "marketplace", "consumer", "sales", "merchant"],
            "media": ["media", "entertainment", "film", "music", "gaming", "publishing", "broadcast", "streaming", "content"],
            "construction": ["construction", "real estate", "building", "property", "architecture", "infrastructure", "housing"],
            "agriculture": ["agriculture", "farming", "food", "crop", "livestock", "agri", "agricultural", "rural"],
            "biotechnology": ["biotech", "biotechnology", "genomics", "life science", "molecular", "genetic", "bio"],
            "government": ["government", "public", "municipal", "federal", "defense", "military", "policy", "regulatory"],
            "telecommunications": ["telecom", "telecommunications", "wireless", "network", "mobile", "5g", "broadband"]
        }

        # Industry synonyms
        industry_synonyms = {
            "it": "technology", "tech": "technology", "software": "technology",
            "medical": "healthcare", "pharma": "healthcare", "health": "healthcare",
            "banking": "finance", "fintech": "finance", "financial": "finance",
            "edu": "education", "academic": "education", "school": "education",
            "mfg": "manufacturing", "industrial": "manufacturing",
            "telco": "telecommunications", "telecom": "telecommunications",
            "agri": "agriculture", "farm": "agriculture",
            "govt": "government", "gov": "government", "public sector": "government"
        }

        # Normalize tag using synonyms
        tag_normalized = tag_lower
        for syn, replacement in industry_synonyms.items():
            if syn in tag_normalized:
                tag_normalized = tag_normalized.replace(syn, replacement)

        best_match = None
        best_score = 0

        for name_lower, item in reference_dict.items():
            score = 0

            # Check for keyword matches with enhanced scoring
            for industry_type, keywords in industry_keywords.items():
                for keyword in keywords:
                    if keyword in tag_normalized and keyword in name_lower:
                        score += len(keyword) * 3
                    elif keyword in tag_normalized and industry_type in name_lower:
                        score += len(keyword) * 2
                    # Check partial matches
                    elif any(word in tag_normalized for word in keyword.split()):
                        score += 1

            # Enhanced similarity with word overlap
            similarity = self.calculate_similarity(tag_normalized, name_lower)

            tag_words = set(tag_normalized.split())
            name_words = set(name_lower.split())
            common_words = tag_words & name_words
            if common_words:
                word_score = len(common_words) / min(len(tag_words), len(name_words)) * 5
                score += word_score

            score += similarity * 10

            if score > best_score:
                best_score = score
                best_match = item["name"]

        return best_match if best_score > 3 else None

    def find_best_audience_match(self, tag: str, reference_dict: Dict[str, Dict]) -> Optional[str]:
        """Find best matching audience using categorization logic"""
        tag_lower = tag.lower()

        # Check for educational institutions first (highest priority)
        educational_patterns = [
            r'.*\b(?:university|universities|college|colleges|school|schools)\b.*',
            r'.*\b(?:student|students|learner|learners|teacher|teachers|educator|educators)\b.*',
            r'.*\b(?:academic|academia|educational|education)\s+(?:institution|organization|body).*',
            r'.*\b(?:faculty|professor|instructor|curriculum)\b.*'
        ]

        for pattern in educational_patterns:
            if re.search(pattern, tag_lower, re.IGNORECASE):
                # Find educational institution matches
                for name_lower, item in reference_dict.items():
                    if re.search(pattern, name_lower, re.IGNORECASE):
                        return item["name"]

        # Enhanced audience keyword patterns with synonyms
        audience_keywords = {
            "researchers": ["research", "researcher", "scientist", "laboratory", "r&d", "phd", "scholar", "academic"],
            "developers": ["developer", "programmer", "engineer", "software", "coding", "dev", "coder", "tech"],
            "healthcare": ["medical", "healthcare", "doctor", "physician", "hospital", "nurse", "clinic", "patient"],
            "enterprise": ["enterprise", "corporate", "business", "company", "organization", "firm", "corporation"],
            "startups": ["startup", "entrepreneur", "innovation", "venture", "founder", "innovator"],
            "government": ["government", "public sector", "policy", "municipal", "federal", "state", "agency"],
            "legal": ["legal", "law", "attorney", "lawyer", "compliance", "regulatory", "counsel"],
            "media": ["media", "entertainment", "journalist", "content creator", "publisher", "broadcaster"],
            "retail": ["retail", "e-commerce", "shopping", "consumer", "marketplace", "store", "merchant"],
            "designers": ["designer", "design", "ui", "ux", "creative", "artist", "graphic"],
            "managers": ["manager", "executive", "director", "ceo", "cto", "leadership", "admin"],
            "students": ["student", "learner", "trainee", "pupil", "apprentice", "intern"],
            "consultants": ["consultant", "advisor", "advisory", "consulting", "expert", "specialist"]
        }

        # Audience synonyms
        audience_synonyms = {
            "dev": "developer", "eng": "engineer", "devs": "developers",
            "med": "medical", "doc": "doctor", "docs": "doctors",
            "corp": "corporate", "biz": "business", "co": "company",
            "gov": "government", "govt": "government",
            "uni": "university", "edu": "education",
            "mgr": "manager", "exec": "executive", "mgmt": "management"
        }

        # Normalize tag using synonyms
        tag_normalized = tag_lower
        for syn, replacement in audience_synonyms.items():
            if syn in tag_normalized:
                tag_normalized = tag_normalized.replace(syn, replacement)

        best_match = None
        best_score = 0

        for name_lower, item in reference_dict.items():
            score = 0

            # Check for keyword matches with enhanced scoring
            for audience_type, keywords in audience_keywords.items():
                for keyword in keywords:
                    if keyword in tag_normalized and keyword in name_lower:
                        score += len(keyword) * 3
                    elif keyword in tag_normalized and audience_type in name_lower:
                        score += len(keyword) * 2
                    # Check partial matches
                    elif any(word in tag_normalized for word in keyword.split()):
                        score += 1

            # Enhanced similarity with word overlap
            similarity = self.calculate_similarity(tag_normalized, name_lower)

            tag_words = set(tag_normalized.split())
            name_words = set(name_lower.split())
            common_words = tag_words & name_words
            if common_words:
                word_score = len(common_words) / min(len(tag_words), len(name_words)) * 5
                score += word_score

            score += similarity * 10

            if score > best_score:
                best_score = score
                best_match = item["name"]

        return best_match if best_score > 3 else None

    def map_tags_simple(self, industries_input: List[str], audience_input: List[str],
                       functions_input: List[str]) -> Dict:
        """Map tags to existing database entries only (no creation of new entries)"""

        # Map input tags to existing entries only
        mapped_industries = []
        for tag in industries_input:
            matched = self.find_best_match(tag, self.industries_dict, "industry")
            if matched:
                mapped_industries.append(matched)
                logger.debug(f"Mapped industry '{tag}' to existing '{matched}'")
            else:
                logger.warning(f"No match found for industry: {tag}")
                self.stats['unmapped_industries'] += 1
                self.stats['total_unmapped'] += 1
                if tag not in self.unmapped_tags['industries']:
                    self.unmapped_tags['industries'].append(tag)

        mapped_audience = []
        for tag in audience_input:
            matched = self.find_best_match(tag, self.audience_dict, "audience")
            if matched:
                mapped_audience.append(matched)
                logger.debug(f"Mapped audience '{tag}' to existing '{matched}'")
            else:
                logger.warning(f"No match found for audience: {tag}")
                self.stats['unmapped_audience'] += 1
                self.stats['total_unmapped'] += 1
                if tag not in self.unmapped_tags['audience']:
                    self.unmapped_tags['audience'].append(tag)

        mapped_functions = []
        for tag in functions_input:
            matched = self.find_best_match(tag, self.functions_dict, "function")
            if matched:
                mapped_functions.append(matched)
                logger.debug(f"Mapped function '{tag}' to existing '{matched}'")
            else:
                logger.warning(f"No match found for business function: {tag}")
                self.stats['unmapped_functions'] += 1
                self.stats['total_unmapped'] += 1
                if tag not in self.unmapped_tags['functions']:
                    self.unmapped_tags['functions'].append(tag)

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

    def get_csv_value(self, row: dict, *possible_keys: str) -> str:
        """
        Safely get value from CSV row trying multiple possible keys.
        Handles variations in column headers like 'mission' vs 'mission ' vs 'mission_'
        """
        for key in possible_keys:
            if key in row and row[key]:
                return row[key].strip()

        # Try with normalized keys (strip spaces, handle underscores)
        for key in possible_keys:
            # Try variations: with/without spaces, with/without underscores
            variations = [
                key,
                key.strip(),
                key + ' ',
                ' ' + key,
                key.replace(' ', '_'),
                key.replace('_', ' '),
                key.replace(' ', ''),
            ]

            for variation in variations:
                if variation in row and row[variation]:
                    return row[variation].strip()

        return ''

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
                'url': self.get_csv_value(row, 'url'),
                'publish_date': self.get_csv_value(row, 'publish_date', 'publish date'),
                'summarized': self.get_csv_value(row, 'summarized'),
                'name': self.get_csv_value(row, 'name', 'title'),
                'mission': self.get_csv_value(row, 'mission', 'mission '),
                'problematics': self.get_csv_value(row, 'problematics'),
                'scope': self.get_csv_value(row, 'scope'),
                'audience': self.get_csv_value(row, 'audience'),
                'how_it_works': self.get_csv_value(row, 'how it works', 'how_it_works', 'how-it-works'),
                'architecture': self.get_csv_value(row, 'architecture'),
                'innovation': self.get_csv_value(row, 'innovation'),
                'use_case': self.get_csv_value(row, 'use case', 'use_case', 'use-case', 'usecase'),
                'industries': self.get_csv_value(row, 'industries', 'industry'),
                'functions': self.get_csv_value(row, 'functions', 'function', 'business_functions'),
                'link': self.get_csv_value(row, 'link', 'url', 'source_url'),
                'date': self.get_csv_value(row, 'date', 'created_date'),
                'team': self.get_csv_value(row, 'team', 'teams', 'university', 'organization')
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
                    industries_input = self.parse_csv_array(smartject_data['industries'], strict_json=False)
                    audience_input = self.parse_csv_array(smartject_data['audience'], strict_json=True)
                    functions_input = self.parse_csv_array(smartject_data['functions'], strict_json=False)
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

                    # Check if audience format was valid (strict JSON required)
                    if not audience_input and smartject_data['audience']:
                        logger.warning(f"  ✕ Skipping {title} — invalid audience format (must be JSON array like [\"Audience1\", \"Audience2\"])")
                        self.stats['skipped'] += 1
                        results.append({
                            'title': title,
                            'status': 'skipped',
                            'reason': 'Invalid audience format - must be JSON array'
                        })
                        continue

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
        summary = (
            f"📊 Processing Summary:\n"
            f"Total records: {self.stats['total']}\n"
            f"✅ Processed: {self.stats['processed']}\n"
            f"🎓 With logos: {self.stats['with_logos']}\n"
            f"⏭️  Skipped (not relevant): {self.stats['skipped_not_relevant']}\n"
            f"⏭️  Skipped (exists): {self.stats['skipped_exists']}\n"
        )

        if self.stats['total_unmapped'] > 0:
            summary += (
                f"\n⚠️  Unmapped tags:\n"
                f"   Industries: {self.stats['unmapped_industries']}\n"
                f"   Audience: {self.stats['unmapped_audience']}\n"
                f"   Functions: {self.stats['unmapped_functions']}\n"
                f"   Total unmapped: {self.stats['total_unmapped']}"
            )

        return summary

    def get_unmapped_tags(self) -> Dict[str, List[str]]:
        """Get all unmapped tags for review"""
        return self.unmapped_tags

    def export_unmapped_tags_csv(self, filepath: str = 'unmapped_tags.csv'):
        """Export unmapped tags to CSV file for review"""
        import csv

        with open(filepath, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Category', 'Unmapped Tag', 'Count'])

            for category, tags in self.unmapped_tags.items():
                for tag in tags:
                    writer.writerow([category, tag, 1])

        logger.info(f"Exported {self.stats['total_unmapped']} unmapped tags to {filepath}")
        return filepath
