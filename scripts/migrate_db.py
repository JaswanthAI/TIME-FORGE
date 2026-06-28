"""
Migration script: Add password_hash and is_admin to faculty table
and insert a master admin account.
Run this ONCE against the existing timeforge_db database.
"""
import asyncio
import asyncpg
import bcrypt
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:admin789@localhost:5432/univ_slots")

async def migrate():
    print("[START] Connecting to database...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"[ERROR] Connection Error: {e}")
        return

    try:
        # 1. Check if password_hash column exists on faculty table
        col_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name='faculty' AND column_name='password_hash'
            )
        """)

        if not col_exists:
            print("[MIGRATE] Adding password_hash column to faculty table...")
            await conn.execute("""
                ALTER TABLE faculty ADD COLUMN password_hash TEXT NOT NULL DEFAULT 'admin789'
            """)
            print("[OK] password_hash column added.")
        else:
            print("[INFO] password_hash column already exists on faculty table.")

        # 2. Check if is_admin column exists on faculty table
        admin_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name='faculty' AND column_name='is_admin'
            )
        """)

        if not admin_exists:
            print("[MIGRATE] Adding is_admin column to faculty table...")
            await conn.execute("""
                ALTER TABLE faculty ADD COLUMN is_admin BOOLEAN DEFAULT FALSE
            """)
            print("[OK] is_admin column added.")
        else:
            print("[INFO] is_admin column already exists on faculty table.")

        # 3. Add ON DELETE CASCADE to slots.subject_id and slots.faculty_id if missing
        # (Safe to skip if already exists — the new schema has them)

        # 4. Insert master admin account
        admin_pw = bcrypt.hashpw("admin789".encode(), bcrypt.gensalt()).decode()
        await conn.execute("""
            INSERT INTO faculty (employee_id, name, department, password_hash, is_admin)
            VALUES ('FACADMIN1', 'Master Admin', 'Administration', $1, TRUE)
            ON CONFLICT (employee_id) DO UPDATE SET 
                password_hash = $1, 
                is_admin = TRUE
        """, admin_pw)
        print("[OK] Master Admin account created/updated (ID: FACADMIN1, password: admin789)")

        print("\n[DONE] Migration complete!")

    except Exception as e:
        print(f"[ERROR] Migration Error: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(migrate())
