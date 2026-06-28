import asyncio
import time
import uuid
import random
import os
from dotenv import load_dotenv
import asyncpg
import httpx
from datetime import datetime

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

# Config
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:admin789@localhost:5432/univ_slots")
API_URL = "http://localhost:8000"
NUM_STUDENTS = 2000
CONCURRENCY = 50 # How many students act at the exact same moment

async def setup_test_students():
    """Bulks creates 2000 test students for the simulation."""
    print(f"🛠  Generating {NUM_STUDENTS} test students in DB...")
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Clear existing test students
    await conn.execute("DELETE FROM students WHERE register_number LIKE 'SIM_%'")
    
    students = []
    for i in range(1, NUM_STUDENTS + 1):
        reg = f"SIM_{i:04d}"
        students.append((reg, f"Test Student {i}", f"test{i}@University.edu", "admin789", "AD", 2))
    
    await conn.executemany("""
        INSERT INTO students (register_number, name, email, password_hash, department, semester)
        VALUES ($1, $2, $3, $4, $5, $6)
    """, students)
    
    # Initialize constraints for these students
    await conn.execute("""
        INSERT INTO student_time_constraints (student_id, subject_id, allowed_days, min_periods_per_week, max_periods_per_week)
        SELECT st.id, sub.id, 31, sub.credits, sub.credits
        FROM students st
        CROSS JOIN subjects sub
        WHERE st.register_number LIKE 'SIM_%'
        ON CONFLICT DO NOTHING
    """)
    
    await conn.close()
    print("✅ Test students ready.")

async def simulate_student(client, student_idx):
    """Simulates a single student workflow: Login -> Prefs -> Select -> Submit."""
    reg = f"SIM_{student_idx:04d}"
    
    try:
        # 1. Login
        start = time.time()
        res = await client.post(f"{API_URL}/api/login", json={
            "register_number": reg,
            "password": "admin789"
        })
        if res.status_code != 200: return False, "Login failed"
        student_id = res.json()["student"]["id"]
        
        # 2. Get Subjects
        res = await client.get(f"{API_URL}/api/subjects")
        subjects = res.json()
        
        # 3. Set Faculty Prefs (pick random faculty for each subject)
        prefs = []
        for s in subjects:
            f_res = await client.get(f"{API_URL}/api/subjects/{s['id']}/faculty")
            faculties = f_res.json()
            if faculties:
                prefs.append({"subject_id": s["id"], "faculty_id": faculties[0]["id"]})
        
        await client.post(f"{API_URL}/api/student/preferences", json={
            "student_id": student_id,
            "preferences": prefs
        })
        
        # 4. Fetch Timetable
        res = await client.get(f"{API_URL}/api/timetable?student_id={student_id}")
        slots = res.json()["slots"]
        
        # 5. Select 2 random slots
        available = [s for s in slots if not s["is_full"]]
        if len(available) >= 2:
            picks = random.sample(available, 2)
            for p in picks:
                await client.post(f"{API_URL}/api/select", json={
                    "student_id": student_id,
                    "slot_id": p["slot_id"]
                })
        
        end = time.time()
        return True, end - start
    except Exception as e:
        return False, str(e)

async def run_simulation():
    await setup_test_students()
    
    print(f"🚀 Starting Stress Test: {NUM_STUDENTS} students, Concurrency: {CONCURRENCY}")
    print(f"Connecting to {API_URL}...")
    
    results = []
    start_time = time.time()
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Process in batches to control concurrency
        for i in range(1, NUM_STUDENTS + 1, CONCURRENCY):
            batch = range(i, min(i + CONCURRENCY, NUM_STUDENTS + 1))
            tasks = [simulate_student(client, idx) for idx in batch]
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)
            print(f"Processed {len(results)} / {NUM_STUDENTS} students...", end="\r")
            
    total_time = time.time() - start_time
    successes = [r for r in results if r[0]]
    failures = [r for r in results if not r[0]]
    avg_lat = sum([r[1] for r in successes]) / len(successes) if successes else 0
    
    print("\n\n" + "="*40)
    print("📊 STRESS TEST RESULTS")
    print("="*40)
    print(f"Total Students:    {NUM_STUDENTS}")
    print(f"Successful:        {len(successes)}")
    print(f"Failed:            {len(failures)}")
    print(f"Total Duration:    {total_time:.2f} seconds")
    print(f"Throughput:        {len(successes)/total_time:.2f} students/sec")
    print(f"Avg Student Loop:  {avg_lat:.2f} seconds")
    print("="*40)

if __name__ == "__main__":
    asyncio.run(run_simulation())
