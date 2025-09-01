import csv
import re

from typing import Dict, List, Optional, Tuple
from pathlib import Path
from utils.logging_config import get_logger

logger = get_logger(__name__)

class LogoMatcher:
    """Service for matching university logos to smartjects"""

    def __init__(self, supabase_client, logos_file: str = "logos/top_30_universities_mentions.csv"):
        self.supabase = supabase_client
        self.logos_file = logos_file
        self.logos_dict = {}
        self.load_university_logos()

    def load_university_logos(self):
        """Load university logos from CSV file"""
        try:
            # Check if file exists
            if not Path(self.logos_file).exists():
                logger.warning(f"Logos file not found: {self.logos_file}")
                return

            self.logos_dict = {}

            with open(self.logos_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file, delimiter=';')

                for row in reader:
                    university = row.get('university', '').strip()
                    logo_url = row.get('logo', '').strip()

                    if university and logo_url:
                        self.logos_dict[university] = logo_url
                        self.logos_dict[university.lower()] = logo_url

            logger.info(f"Loaded {len(set(self.logos_dict.values()))} unique university logos")

        except Exception as e:
            logger.error(f"Error loading university logos: {e}")

    def normalize_university_name(self, name: str) -> str:
        """Normalize university name for better matching"""
        name = name.lower()
        # Remove common prefixes and suffixes
        name = re.sub(r'^(the\s+)?', '', name)
        name = re.sub(r'\s+(university|college|institute|school)(\s+of\s+[^,]+)?$', '', name)
        name = re.sub(r'\s+of\s+technology$', '', name)
        name = re.sub(r'\s+of\s+science\s+and\s+technology$', '', name)
        name = re.sub(r'\s+\([^)]+\)$', '', name)  # Remove parentheses
        name = re.sub(r'\s+', ' ', name).strip()
        return name

    def find_matching_logo(self, team_universities: List[str]) -> Optional[Tuple[str, str, str]]:
        """Find matching logo for any university in the team"""
        if not self.logos_dict:
            return None

        for university in team_universities:
            university = university.strip()

            # Skip if empty or too short
            if not university or len(university) < 5:
                continue

            # 1. Direct match
            if university in self.logos_dict:
                return (university, self.logos_dict[university], 'direct')

            # 2. Case insensitive match
            if university.lower() in self.logos_dict:
                return (university, self.logos_dict[university.lower()], 'case_insensitive')

            # 3. Partial matching
            university_lower = university.lower()
            for logo_uni, logo_url in self.logos_dict.items():
                if logo_uni == logo_uni.lower():  # Skip lowercase duplicates
                    continue

                logo_uni_lower = logo_uni.lower()

                # Check if university name contains logo university name or vice versa
                if len(university_lower) > 10 and len(logo_uni_lower) > 10:
                    if university_lower in logo_uni_lower or logo_uni_lower in university_lower:
                        return (university, logo_url, 'partial')

            # 4. Normalized matching
            normalized_university = self.normalize_university_name(university)
            if len(normalized_university) > 3:
                for logo_uni, logo_url in self.logos_dict.items():
                    if logo_uni == logo_uni.lower():  # Skip lowercase duplicates
                        continue

                    normalized_logo_uni = self.normalize_university_name(logo_uni)

                    if normalized_university == normalized_logo_uni:
                        return (university, logo_url, 'normalized_exact')

                    # Check if one contains the other after normalization
                    if len(normalized_university) > 5 and len(normalized_logo_uni) > 5:
                        if normalized_university in normalized_logo_uni or normalized_logo_uni in normalized_university:
                            return (university, logo_url, 'normalized_partial')

        return None

    def reload_logos(self, logos_file: Optional[str] = None):
        """Reload logos from file"""
        if logos_file:
            self.logos_file = logos_file
        self.load_university_logos()

    def get_available_universities(self) -> List[str]:
        """Get list of available universities"""
        unique_universities = {k for k in self.logos_dict if k != k.lower()}
        return sorted(list(unique_universities))

    def update_existing_smartject_logos(self, dry_run: bool = False) -> Dict:
        """Update logos for existing smartjects in database"""
        stats = {
            'total_smartjects': 0,
            'with_teams': 0,
            'found_matches': 0,
            'already_correct': 0,
            'updated': 0,
            'errors': 0
        }

        # Fetch all smartjects
        smartjects = self.supabase.fetch_all_smartjects()
        stats['total_smartjects'] = len(smartjects)

        logger.info(f"Found {len(smartjects)} smartjects to process")

        for smartject in smartjects:
            smartject_id = smartject.get('id')
            title = smartject.get('title', '')
            team = smartject.get('team', [])
            current_image_url = smartject.get('image_url', '')

            # Skip if no team data
            if not team or not isinstance(team, list):
                continue

            stats['with_teams'] += 1

            # Find matching logo
            match_result = self.find_matching_logo(team)

            if match_result:
                matched_university, logo_url, match_type = match_result
                stats['found_matches'] += 1

                # Check if already has the same logo
                if logo_url == current_image_url:
                    stats['already_correct'] += 1
                    continue

                # Update in database if not dry run
                if not dry_run:
                    try:
                        if self.supabase.update_smartject_logo(smartject_id, logo_url):
                            stats['updated'] += 1
                            logger.info(f"Updated logo for {title} ({match_type})")
                        else:
                            stats['errors'] += 1

                    except Exception as e:
                        stats['errors'] += 1
                        logger.error(f"Error updating {title}: {e}")

        return stats
