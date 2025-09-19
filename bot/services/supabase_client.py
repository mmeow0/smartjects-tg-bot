import os
from typing import List, Dict, Optional, Set
from supabase import create_client, Client
from dotenv import load_dotenv
from utils.logging_config import get_logger

load_dotenv()

logger = get_logger(__name__)

class SupabaseClient:
    """Client for interacting with Supabase database"""

    def __init__(self):
        self.url = os.getenv("SUPABASE_URL", "http://127.0.0.1:54321")
        self.key = os.getenv("SUPABASE_KEY", "")
        self.client: Client = create_client(self.url, self.key)

    def fetch_reference_table(self, table_name: str) -> List[Dict]:
        """Fetch all records from a reference table with pagination"""
        all_records = []
        page_size = 1000
        offset = 0

        try:
            while True:
                response = self.client.table(table_name) \
                    .select("*") \
                    .range(offset, offset + page_size - 1) \
                    .execute()

                data = response.data or []
                if not data:
                    break

                all_records.extend(data)
                offset += page_size

                # Log progress for large datasets
                if len(all_records) % 5000 == 0:
                    logger.info(f"  Loaded {len(all_records)} records from {table_name}...")

            if not all_records:
                logger.warning(f"No data found in {table_name}")
            else:
                logger.debug(f"Loaded {len(all_records)} total records from {table_name}")

            return all_records
        except Exception as e:
            logger.error(f"Error fetching {table_name}: {e}")
            return []

    def fetch_existing_titles(self) -> Set[str]:
        """Fetch all existing smartject titles"""
        logger.debug("Starting fetch_existing_titles()")
        try:
            logger.debug("Querying smartjects table for titles...")
            response = self.client.table("smartjects").select("title").execute()

            logger.debug(f"Query response - data count: {len(response.data) if response.data else 0}")
            logger.debug(f"Query response - raw data sample: {response.data[:3] if response.data else 'None'}")

            if not response.data:
                logger.warning("No existing smartjects found in database - response.data is empty!")
                logger.debug(f"Full response object: {response}")
                return set()

            existing_titles = set()
            skipped_count = 0

            for i, item in enumerate(response.data):
                if item.get("title"):
                    # Normalize title: strip whitespace, convert to lowercase
                    raw_title = item["title"]
                    normalized_title = raw_title.strip().lower()
                    existing_titles.add(normalized_title)

                    # Log first few titles for debugging
                    if i < 5:
                        logger.debug(f"  Title {i+1}: '{raw_title}' -> '{normalized_title}'")
                else:
                    skipped_count += 1
                    logger.debug(f"  Skipped item {i+1} - no title field: {item}")

            if skipped_count > 0:
                logger.warning(f"Skipped {skipped_count} items without title field")

            logger.info(f"Fetched {len(existing_titles)} existing smartject titles from {len(response.data)} records")
            logger.debug(f"Sample of normalized titles: {list(existing_titles)[:5]}")
            return existing_titles
        except Exception as e:
            logger.error(f"Error fetching existing titles: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            logger.error("Returning empty set due to error")
            return set()

    def insert_smartject(self, smartject_data: Dict) -> Optional[Dict]:
        """Insert a new smartject into the database or return existing one"""
        try:
            # Log the title being inserted for debugging
            title = smartject_data.get('title', 'Unknown')
            logger.debug(f"Attempting to insert smartject: {title}")

            # First, try to find existing smartject by title
            existing = self.client.table("smartjects").select("*").eq("title", title).execute()
            if existing.data and len(existing.data) > 0:
                logger.debug(f"Smartject '{title}' already exists, returning existing with ID: {existing.data[0]['id']}")
                return existing.data[0]

            # If not exists, insert new smartject
            response = self.client.table("smartjects").insert(smartject_data).execute()

            if response.data and len(response.data) > 0:
                # logger.info(f"Successfully inserted smartject: {title}")
                return response.data[0]

            # If insert didn't return data but succeeded, fetch it
            created = self.client.table("smartjects").select("*").eq("id", smartject_data.get('id')).execute()
            if created.data and len(created.data) > 0:
                return created.data[0]

            return None
        except Exception as e:
            logger.error(f"Error inserting smartject '{smartject_data.get('title', 'Unknown')}': {e}")
            # Check if it's a duplicate key error and try to fetch existing
            if "duplicate key" in str(e).lower() or "already exists" in str(e).lower():
                logger.debug("Duplicate detected, fetching existing smartject")
                title = smartject_data.get('title', 'Unknown')
                existing = self.client.table("smartjects").select("*").eq("title", title).execute()
                if existing.data and len(existing.data) > 0:
                    return existing.data[0]
            return None

    def insert_smartject_industries(self, relations: List[Dict]) -> bool:
        """Insert smartject-industry relations with robust duplicate handling"""
        if not relations:
            return True

        try:
            # Process relations in batches for efficiency
            batch_size = 50
            successful_inserts = 0

            for i in range(0, len(relations), batch_size):
                batch = relations[i:i + batch_size]

                # Check which relations already exist in this batch
                existing_relations = set()
                for relation in batch:
                    smartject_id = relation['smartject_id']
                    industry_id = relation['industry_id']

                    try:
                        existing = self.client.table("smartject_industries").select("smartject_id").eq(
                            "smartject_id", smartject_id
                        ).eq("industry_id", industry_id).limit(1).execute()

                        if existing.data:
                            existing_relations.add((smartject_id, industry_id))
                            logger.debug(f"Industry relation already exists: {smartject_id} -> {industry_id}")
                    except Exception as check_error:
                        logger.warning(f"Error checking existing industry relation {smartject_id} -> {industry_id}: {check_error}")
                        # Continue with insertion attempt even if check fails

                # Insert only non-existing relations
                relations_to_insert = []
                for relation in batch:
                    smartject_id = relation['smartject_id']
                    industry_id = relation['industry_id']

                    if (smartject_id, industry_id) not in existing_relations:
                        relations_to_insert.append(relation)

                # Insert new relations one by one to handle individual conflicts
                for relation in relations_to_insert:
                    smartject_id = relation['smartject_id']
                    industry_id = relation['industry_id']

                    try:
                        self.client.table("smartject_industries").insert(relation).execute()
                        successful_inserts += 1
                        logger.debug(f"Successfully inserted industry relation: {smartject_id} -> {industry_id}")

                    except Exception as insert_error:
                        error_str = str(insert_error).lower()

                        # Handle different types of constraint violations
                        if any(keyword in error_str for keyword in ['duplicate key', 'unique constraint', 'already exists']):
                            logger.info(f"Duplicate industry relation skipped (concurrent insert?): {smartject_id} -> {industry_id}")
                            # Don't count as error - this is expected behavior

                        elif 'foreign key' in error_str:
                            logger.error(f"Foreign key constraint violation for industry relation {smartject_id} -> {industry_id}: {insert_error}")
                            # Continue with other relations

                        else:
                            logger.error(f"Unexpected error inserting industry relation {smartject_id} -> {industry_id}: {insert_error}")
                            # Continue with other relations rather than failing completely

            logger.info(f"Inserted {successful_inserts} new smartject-industry relations out of {len(relations)} total")
            return True

        except Exception as e:
            logger.error(f"Critical error in insert_smartject_industries: {e}")
            return False

    def insert_industry(self, name: str) -> Optional[Dict]:
        """Insert a new industry entry and return it with its ID, or return existing if found"""
        try:
            # First check if industry already exists (case-insensitive)
            existing = self.client.table("industries").select("*").ilike("name", name).execute()
            if existing.data:
                logger.debug(f"Industry '{name}' already exists, returning existing")
                return {"id": existing.data[0]["id"], "name": existing.data[0]["name"]}

            # Create new if doesn't exist
            result = self.client.table("industries").insert({"name": name}).execute()
            if result.data and len(result.data) > 0:
                return {"id": result.data[0]["id"], "name": result.data[0]["name"]}
            return None
        except Exception as e:
            logger.error(f"Error inserting industry '{name}': {e}")
            return None

    def insert_audience(self, name: str) -> Optional[Dict]:
        """Insert a new audience entry and return it with its ID, or return existing if found"""
        try:
            # First check if audience already exists (case-insensitive)
            existing = self.client.table("audience").select("*").ilike("name", name).execute()
            if existing.data:
                logger.debug(f"Audience '{name}' already exists, returning existing")
                return {"id": existing.data[0]["id"], "name": existing.data[0]["name"]}

            # Create new if doesn't exist
            result = self.client.table("audience").insert({"name": name}).execute()
            if result.data and len(result.data) > 0:
                return {"id": result.data[0]["id"], "name": result.data[0]["name"]}
            return None
        except Exception as e:
            logger.error(f"Error inserting audience '{name}': {e}")
            return None

    def insert_business_function(self, name: str) -> Optional[Dict]:
        """Insert a new business function entry and return it with its ID, or return existing if found"""
        try:
            # First check if business function already exists (case-insensitive)
            existing = self.client.table("business_functions").select("*").ilike("name", name).execute()
            if existing.data:
                logger.debug(f"Business function '{name}' already exists, returning existing")
                return {"id": existing.data[0]["id"], "name": existing.data[0]["name"]}

            # Create new if doesn't exist
            result = self.client.table("business_functions").insert({"name": name}).execute()
            if result.data and len(result.data) > 0:
                return {"id": result.data[0]["id"], "name": result.data[0]["name"]}
            return None
        except Exception as e:
            logger.error(f"Error inserting business function '{name}': {e}")
            return None

    def insert_smartject_audience(self, relations: List[Dict]) -> bool:
        """Insert smartject-audience relations with robust duplicate handling"""
        if not relations:
            return True

        try:
            # Process relations in batches for efficiency
            batch_size = 50
            successful_inserts = 0

            for i in range(0, len(relations), batch_size):
                batch = relations[i:i + batch_size]

                # Check which relations already exist in this batch
                existing_relations = set()
                for relation in batch:
                    smartject_id = relation['smartject_id']
                    audience_id = relation['audience_id']

                    try:
                        existing = self.client.table("smartject_audience").select("smartject_id").eq(
                            "smartject_id", smartject_id
                        ).eq("audience_id", audience_id).limit(1).execute()

                        if existing.data:
                            existing_relations.add((smartject_id, audience_id))
                            logger.debug(f"Relation already exists: {smartject_id} -> {audience_id}")
                    except Exception as check_error:
                        logger.warning(f"Error checking existing relation {smartject_id} -> {audience_id}: {check_error}")
                        # Continue with insertion attempt even if check fails

                # Insert only non-existing relations
                relations_to_insert = []
                for relation in batch:
                    smartject_id = relation['smartject_id']
                    audience_id = relation['audience_id']

                    if (smartject_id, audience_id) not in existing_relations:
                        relations_to_insert.append(relation)

                # Insert new relations one by one to handle individual conflicts
                for relation in relations_to_insert:
                    smartject_id = relation['smartject_id']
                    audience_id = relation['audience_id']

                    try:
                        self.client.table("smartject_audience").insert(relation).execute()
                        successful_inserts += 1
                        logger.debug(f"Successfully inserted relation: {smartject_id} -> {audience_id}")

                    except Exception as insert_error:
                        error_str = str(insert_error).lower()

                        # Handle different types of constraint violations
                        if any(keyword in error_str for keyword in ['duplicate key', 'unique constraint', 'already exists']):
                            logger.info(f"Duplicate relation skipped (concurrent insert?): {smartject_id} -> {audience_id}")
                            # Don't count as error - this is expected behavior

                        elif 'foreign key' in error_str:
                            logger.error(f"Foreign key constraint violation for relation {smartject_id} -> {audience_id}: {insert_error}")
                            # Continue with other relations

                        else:
                            logger.error(f"Unexpected error inserting relation {smartject_id} -> {audience_id}: {insert_error}")
                            # Continue with other relations rather than failing completely

            logger.info(f"Inserted {successful_inserts} new smartject-audience relations out of {len(relations)} total")
            return True

        except Exception as e:
            logger.error(f"Critical error in insert_smartject_audience: {e}")
            return False

    def insert_smartject_functions(self, relations: List[Dict]) -> bool:
        """Insert smartject-business function relations with robust duplicate handling"""
        if not relations:
            return True

        try:
            # Process relations in batches for efficiency
            batch_size = 50
            successful_inserts = 0

            for i in range(0, len(relations), batch_size):
                batch = relations[i:i + batch_size]

                # Check which relations already exist in this batch
                existing_relations = set()
                for relation in batch:
                    smartject_id = relation['smartject_id']
                    function_id = relation['function_id']

                    try:
                        existing = self.client.table("smartject_business_functions").select("smartject_id").eq(
                            "smartject_id", smartject_id
                        ).eq("function_id", function_id).limit(1).execute()

                        if existing.data:
                            existing_relations.add((smartject_id, function_id))
                            logger.debug(f"Function relation already exists: {smartject_id} -> {function_id}")
                    except Exception as check_error:
                        logger.warning(f"Error checking existing function relation {smartject_id} -> {function_id}: {check_error}")
                        # Continue with insertion attempt even if check fails

                # Insert only non-existing relations
                relations_to_insert = []
                for relation in batch:
                    smartject_id = relation['smartject_id']
                    function_id = relation['function_id']

                    if (smartject_id, function_id) not in existing_relations:
                        relations_to_insert.append(relation)

                # Insert new relations one by one to handle individual conflicts
                for relation in relations_to_insert:
                    smartject_id = relation['smartject_id']
                    function_id = relation['function_id']

                    try:
                        self.client.table("smartject_business_functions").insert(relation).execute()
                        successful_inserts += 1
                        logger.debug(f"Successfully inserted function relation: {smartject_id} -> {function_id}")

                    except Exception as insert_error:
                        error_str = str(insert_error).lower()

                        # Handle different types of constraint violations
                        if any(keyword in error_str for keyword in ['duplicate key', 'unique constraint', 'already exists']):
                            logger.info(f"Duplicate function relation skipped (concurrent insert?): {smartject_id} -> {function_id}")
                            # Don't count as error - this is expected behavior

                        elif 'foreign key' in error_str:
                            logger.error(f"Foreign key constraint violation for function relation {smartject_id} -> {function_id}: {insert_error}")
                            # Continue with other relations

                        else:
                            logger.error(f"Unexpected error inserting function relation {smartject_id} -> {function_id}: {insert_error}")
                            # Continue with other relations rather than failing completely

            logger.info(f"Inserted {successful_inserts} new smartject-function relations out of {len(relations)} total")
            return True

        except Exception as e:
            logger.error(f"Critical error in insert_smartject_functions: {e}")
            return False

    def update_smartject_logo(self, smartject_id: str, logo_url: str) -> bool:
        """Update smartject logo URL"""
        try:
            self.client.table("smartjects").update({
                "image_url": logo_url
            }).eq("id", smartject_id).execute()
            return True
        except Exception as e:
            logger.error(f"Error updating logo for smartject {smartject_id}: {e}")
            return False

    def fetch_all_smartjects(self) -> List[Dict]:
        """Fetch all smartjects with pagination"""
        all_smartjects = []
        page_size = 1000
        offset = 0

        try:
            while True:
                response = self.client.table("smartjects") \
                    .select("id, title, team, image_url, audience") \
                    .range(offset, offset + page_size - 1) \
                    .execute()

                data = response.data or []
                if not data:
                    break

                all_smartjects.extend(data)
                offset += page_size

                # Log progress for large datasets
                if len(all_smartjects) % 5000 == 0:
                    logger.info(f"  Loaded {len(all_smartjects)} smartjects...")

            logger.debug(f"Loaded {len(all_smartjects)} total smartjects")
            return all_smartjects
        except Exception as e:
            logger.error(f"Error fetching smartjects: {e}")
            return []

    def fetch_existing_teams(self) -> Dict[str, str]:
        """Fetch all existing teams as a dict {name: id}"""
        try:
            response = self.client.table("teams").select("id, name").execute()
            if not response.data:
                return {}
            return {team["name"]: team["id"] for team in response.data}
        except Exception as e:
            logger.error(f"Error fetching teams: {e}")
            return {}

    def insert_teams(self, team_names: List[str]) -> Dict[str, str]:
        """Insert new teams and return mapping of team name to ID"""
        if not team_names:
            return {}

        try:
            # Prepare team data
            teams_data = [{"name": name} for name in team_names]

            # Insert teams
            response = self.client.table("teams").insert(teams_data).execute()

            if response.data:
                return {team["name"]: team["id"] for team in response.data}
            return {}
        except Exception as e:
            logger.error(f"Error inserting teams: {e}")
            return {}

    def insert_smartject_teams(self, relations: List[Dict]) -> bool:
        """Insert smartject-team relations"""
        try:
            if relations:
                # Use upsert to handle duplicates
                self.client.table("smartject_teams").upsert(relations).execute()
            return True
        except Exception as e:
            logger.error(f"Error inserting smartject-team relations: {e}")
            return False

    def sync_teams_for_smartject(self, smartject_id: str, team_names: List[str]) -> bool:
        """Sync teams for a specific smartject"""
        if not team_names:
            return True

        try:
            # Get existing teams
            existing_teams = self.fetch_existing_teams()

            # Find teams that need to be created
            new_team_names = [name for name in team_names if name not in existing_teams]

            # Insert new teams if any
            if new_team_names:
                new_teams_mapping = self.insert_teams(new_team_names)
                existing_teams.update(new_teams_mapping)

            # Create smartject-team relations
            relations = []
            for team_name in team_names:
                if team_name in existing_teams:
                    relations.append({
                        "smartject_id": smartject_id,
                        "team_id": existing_teams[team_name]
                    })

            # Insert relations
            return self.insert_smartject_teams(relations)

        except Exception as e:
            logger.error(f"Error syncing teams for smartject {smartject_id}: {e}")
            return False

    def batch_sync_all_teams(self) -> Dict[str, int]:
        """Batch synchronize all teams from smartjects - more efficient approach"""
        stats = {
            'new_teams': 0,
            'new_relations': 0,
            'errors': 0
        }

        try:
            # 1. Fetch all unique team names from smartjects
            logger.info("Fetching all team names from smartjects...")
            response = self.client.rpc("get_distinct_team_names").execute()

            if not response.data:
                # Fallback to manual extraction
                smartjects = self.fetch_all_smartjects()
                all_teams = set()
                for smartject in smartjects:
                    teams = smartject.get('team', [])
                    if teams and isinstance(teams, list):
                        all_teams.update(teams)
                unique_team_names = list(all_teams)
            else:
                unique_team_names = response.data

            logger.info(f"Found {len(unique_team_names)} unique team names")

            # 2. Get existing teams
            existing_teams = self.fetch_existing_teams()
            existing_names = set(existing_teams.keys())

            # 3. Find new teams to insert
            new_team_names = [name for name in unique_team_names if name and name not in existing_names]

            # 4. Insert new teams
            if new_team_names:
                logger.info(f"Inserting {len(new_team_names)} new teams...")
                new_teams_mapping = self.insert_teams(new_team_names)
                stats['new_teams'] = len(new_teams_mapping)
                existing_teams.update(new_teams_mapping)

            # 5. Create smartject-team relations
            logger.info("Creating smartject-team relations...")

            # Fetch all smartjects with teams
            smartjects = self.fetch_all_smartjects()

            # Prepare all relations
            all_relations = []
            for smartject in smartjects:
                smartject_id = smartject.get('id')
                teams = smartject.get('team', [])

                if teams and isinstance(teams, list):
                    for team_name in teams:
                        if team_name in existing_teams:
                            all_relations.append({
                                "smartject_id": smartject_id,
                                "team_id": existing_teams[team_name]
                            })

            # Insert relations in batches
            if all_relations:
                batch_size = 1000
                for i in range(0, len(all_relations), batch_size):
                    batch = all_relations[i:i + batch_size]
                    try:
                        self.client.table("smartject_teams").upsert(batch).execute()
                        stats['new_relations'] += len(batch)
                    except Exception as e:
                        logger.error(f"Error inserting batch of relations: {e}")
                        stats['errors'] += 1

            logger.info(f"Teams sync complete: {stats['new_teams']} new teams, {stats['new_relations']} relations")
            return stats

        except Exception as e:
            logger.error(f"Error in batch teams sync: {e}")
            stats['errors'] += 1
            return stats

    def search_smartjects_by_title(self, query: str) -> List[Dict]:
        """Search smartjects by title (case-insensitive partial match)"""
        try:
            # Use ilike for case-insensitive partial matching
            response = self.client.table("smartjects") \
                .select("id, title, mission, created_at") \
                .ilike("title", f"%{query}%") \
                .limit(10) \
                .execute()

            return response.data or []
        except Exception as e:
            logger.error(f"Error searching smartjects: {e}")
            return []

    def get_smartject_details(self, smartject_id: str) -> Optional[Dict]:
        """Get full smartject details including all relations"""
        try:
            # Get main smartject data
            response = self.client.table("smartjects") \
                .select("*") \
                .eq("id", smartject_id) \
                .single() \
                .execute()

            if not response.data:
                return None

            smartject = response.data

            # Get industries
            industries_response = self.client.table("smartject_industries") \
                .select("industry_id, industries(id, name)") \
                .eq("smartject_id", smartject_id) \
                .execute()
            smartject['industries'] = [item['industries'] for item in (industries_response.data or [])]

            # Get audience
            audience_response = self.client.table("smartject_audience") \
                .select("audience_id, audience(id, name)") \
                .eq("smartject_id", smartject_id) \
                .execute()
            smartject['audience_list'] = [item['audience'] for item in (audience_response.data or [])]

            # Get business functions
            functions_response = self.client.table("smartject_business_functions") \
                .select("function_id, business_functions(id, name)") \
                .eq("smartject_id", smartject_id) \
                .execute()
            smartject['business_functions'] = [item['business_functions'] for item in (functions_response.data or [])]

            # Get teams
            teams_response = self.client.table("smartject_teams") \
                .select("team_id, teams(id, name)") \
                .eq("smartject_id", smartject_id) \
                .execute()
            smartject['teams_list'] = [item['teams'] for item in (teams_response.data or [])]

            return smartject
        except Exception as e:
            logger.error(f"Error getting smartject details: {e}")
            return None

    def update_smartject(self, smartject_id: str, update_data: Dict) -> bool:
        """Update smartject main data"""
        try:
            # Remove relation fields if present (they need special handling)
            core_fields = {k: v for k, v in update_data.items()
                          if k not in ['industries', 'audience', 'business_functions', 'teams']}

            # Update main smartject data
            if core_fields:
                response = self.client.table("smartjects") \
                    .update(core_fields) \
                    .eq("id", smartject_id) \
                    .execute()

                if not response.data:
                    return False

            # Handle relations updates if provided
            if 'industries' in update_data:
                self._update_smartject_industries(smartject_id, update_data['industries'])

            if 'audience' in update_data:
                self._update_smartject_audience(smartject_id, update_data['audience'])

            if 'business_functions' in update_data:
                self._update_smartject_functions(smartject_id, update_data['business_functions'])

            if 'teams' in update_data:
                self._update_smartject_teams(smartject_id, update_data['teams'])

            logger.info(f"Successfully updated smartject {smartject_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating smartject {smartject_id}: {e}")
            return False

    def _update_smartject_industries(self, smartject_id: str, industry_ids: List[str]):
        """Update smartject-industry relations"""
        try:
            # Delete existing relations
            self.client.table("smartject_industries") \
                .delete() \
                .eq("smartject_id", smartject_id) \
                .execute()

            # Insert new relations
            if industry_ids:
                relations = [{"smartject_id": smartject_id, "industry_id": ind_id}
                           for ind_id in industry_ids]
                self.insert_smartject_industries(relations)
        except Exception as e:
            logger.error(f"Error updating industries for smartject {smartject_id}: {e}")

    def _update_smartject_audience(self, smartject_id: str, audience_ids: List[str]):
        """Update smartject-audience relations"""
        try:
            # Delete existing relations
            self.client.table("smartject_audience") \
                .delete() \
                .eq("smartject_id", smartject_id) \
                .execute()

            # Insert new relations
            if audience_ids:
                relations = [{"smartject_id": smartject_id, "audience_id": aud_id}
                           for aud_id in audience_ids]
                self.insert_smartject_audience(relations)
        except Exception as e:
            logger.error(f"Error updating audience for smartject {smartject_id}: {e}")

    def _update_smartject_functions(self, smartject_id: str, function_ids: List[str]):
        """Update smartject-business function relations"""
        try:
            # Delete existing relations
            self.client.table("smartject_business_functions") \
                .delete() \
                .eq("smartject_id", smartject_id) \
                .execute()

            # Insert new relations
            if function_ids:
                relations = [{"smartject_id": smartject_id, "function_id": func_id}
                           for func_id in function_ids]
                self.insert_smartject_functions(relations)
        except Exception as e:
            logger.error(f"Error updating functions for smartject {smartject_id}: {e}")

    def _update_smartject_teams(self, smartject_id: str, team_names: List[str]):
        """Update smartject-team relations"""
        try:
            # Delete existing relations
            self.client.table("smartject_teams") \
                .delete() \
                .eq("smartject_id", smartject_id) \
                .execute()

            # Sync teams (create if needed and add relations)
            self.sync_teams_for_smartject(smartject_id, team_names)
        except Exception as e:
            logger.error(f"Error updating teams for smartject {smartject_id}: {e}")

    def delete_smartject(self, smartject_id: str) -> bool:
        """Delete a smartject and all its relations"""
        try:
            # Delete relations first (due to foreign key constraints)

            # Delete industry relations
            self.client.table("smartject_industries") \
                .delete() \
                .eq("smartject_id", smartject_id) \
                .execute()

            # Delete audience relations
            self.client.table("smartject_audience") \
                .delete() \
                .eq("smartject_id", smartject_id) \
                .execute()

            # Delete function relations
            self.client.table("smartject_business_functions") \
                .delete() \
                .eq("smartject_id", smartject_id) \
                .execute()

            # Delete team relations
            self.client.table("smartject_teams") \
                .delete() \
                .eq("smartject_id", smartject_id) \
                .execute()

            # Finally delete the smartject itself
            response = self.client.table("smartjects") \
                .delete() \
                .eq("id", smartject_id) \
                .execute()

            if response.data:
                logger.info(f"Successfully deleted smartject {smartject_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting smartject {smartject_id}: {e}")
            return False
