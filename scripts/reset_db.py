import asyncio
import asyncpg
import os
from dotenv import load_dotenv

# Load environment variables (look in current dir or parent dir)
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:admin789@localhost:5432/univ_slots")
# Updated to find schema.sql in the parent directory
SCHEMA_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "schema.sql")

async def reset_db():
    print("Connecting to database...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        return

    print(f"Reading schema from {SCHEMA_FILE}...")
    if not os.path.exists(SCHEMA_FILE):
        print(f"Error: Schema file not found at {SCHEMA_FILE}")
        return

    with open(SCHEMA_FILE, "r") as f:
        schema_sql = f.read()

    print("Executing schema...")
    try:
        await conn.execute(schema_sql)
        print("Database reset and seeded successfully.")
    except Exception as e:
        print(f"Error executing schema: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(reset_db())
