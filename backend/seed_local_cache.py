import os
import sys
import logging
from supabase import create_client
from local_cache import init_dbs, seed_tracks_from_supabase, seed_links_from_supabase

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Use the same credentials as in cache_web.py (keep as is, per user request)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

def main():
    try:
        logger.info("Initializing local databases...")
        init_dbs()
        
        logger.info("Creating Supabase client...")
        supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        
        # Optional: test connection by fetching a single row
        try:
            supabase.table("tracks").select("spotify_uri").limit(1).execute()
        except Exception as e:
            logger.error(f"Cannot connect to Supabase or 'tracks' table missing: {e}")
            sys.exit(1)
        
        logger.info("Seeding tracks from Supabase...")
        seed_tracks_from_supabase(supabase)
        
        logger.info("Seeding links from Supabase...")
        seed_links_from_supabase(supabase)
        
        logger.info("✅ Local cache seeded successfully. You can now restart the service.")
        
    except KeyboardInterrupt:
        logger.warning("Seeding interrupted by user.")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Seeding failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()