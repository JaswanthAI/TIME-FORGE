# TimeForge Faculty Slot Selection System

## Quick Start

### 1. PostgreSQL Setup
```bash
createdb timeforge_db
psql timeforge_db < schema.sql
```

### 2. Python Backend
```bash
pip install -r requirements.txt
export DATABASE_URL="postgresql://postgres:YOUR_PASSWORD@localhost:5432/timeforge_db"
python main.py
# Server starts at http://localhost:8000
```

> Important: Do not commit your `.env` file to GitHub. A `.gitignore` is already included to ignore `.env`, virtual environments, and editor files.

### 3. Seed sample slots (dev only)
```bash
curl -X POST http://localhost:8000/api/admin/seed-slots
```

### 4. Frontend
Open `login_page.html` in browser (or serve via nginx/any static server).
After login, it redirects to `timetable.html`.

---

## File Structure
```
login_page.html      ← existing login (unchanged)
timetable.html       ← slot selection UI
main.py              ← FastAPI backend
schema.sql           ← PostgreSQL schema + seed data
requirements.txt     ← Python dependencies
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /api/login | Student authentication |
| GET | /api/timetable?student_id= | Full timetable with capacity |
| GET | /api/subjects | All subjects |
| GET | /api/subjects/{id}/faculty | Faculty for a subject |
| POST | /api/select | Select a slot |
| DELETE | /api/select | Deselect a slot |
| GET | /api/validate/{student_id} | Validate min/max constraints |
| POST | /api/submit | Lock selection permanently |
| GET | /api/export/selections | Download XLS report |

---

## Race Condition Prevention
Slot selection uses `SELECT ... FOR UPDATE` row-level locking inside a transaction.
Simultaneous requests for the last seat are serialized at DB level — only one succeeds.

## Constraint Enforcement (two layers)
1. **API layer** — validates min/max periods/week before accepting `submit`
2. **DB layer** — UNIQUE constraints prevent double-booking; CHECK constraints prevent invalid day/period values

---

## XLS Export
`GET /api/export/selections` — downloads all **submitted** selections as a formatted Excel file.
Columns: Register Number, Student Name, Department, Subject Code, Subject Name, Faculty, Day, Period, Time, Selected At
