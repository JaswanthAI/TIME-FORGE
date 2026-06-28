-- ============================================================
-- University Faculty Slot Selection System — PostgreSQL Schema (v2)
-- Student-Centric Timetable with Bundle-Based Scheduling
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Clear existing schema for a clean reset
DROP VIEW IF EXISTS slot_capacity CASCADE;
DROP TABLE IF EXISTS student_submissions CASCADE;
DROP TABLE IF EXISTS student_faculty_preferences CASCADE;
DROP TABLE IF EXISTS student_selections CASCADE;
DROP TABLE IF EXISTS student_time_constraints CASCADE;
DROP TABLE IF EXISTS slots CASCADE;
DROP TABLE IF EXISTS faculty_availability CASCADE;
DROP TABLE IF EXISTS faculty_subjects CASCADE;
DROP TABLE IF EXISTS semester_subjects CASCADE;
DROP TABLE IF EXISTS faculty CASCADE;
DROP TABLE IF EXISTS subjects CASCADE;
DROP TABLE IF EXISTS periods CASCADE;
DROP TABLE IF EXISTS students CASCADE;
DROP TABLE IF EXISTS system_settings CASCADE;

-- ============================================================
-- Core Tables
-- ============================================================

-- Students
CREATE TABLE students (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    register_number VARCHAR(20) UNIQUE NOT NULL,
    name            VARCHAR(100) NOT NULL,
    email           VARCHAR(150) UNIQUE,
    password_hash   TEXT NOT NULL,
    department      VARCHAR(50),
    semester        INTEGER,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Subjects (with type and actual periods needed)
CREATE TABLE subjects (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code             VARCHAR(20) UNIQUE NOT NULL,
    name             VARCHAR(100) NOT NULL,
    credits          INTEGER DEFAULT 3,
    subject_type     VARCHAR(10) DEFAULT 'theory' CHECK (subject_type IN ('theory', 'lab')),
    periods_per_week INTEGER, -- theory: same as credits, lab: 2 consecutive periods
    cluster_id       INTEGER NOT NULL DEFAULT 1 -- 1-5 for FPC algorithm
);

-- Faculty (with auth columns)
CREATE TABLE faculty (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    employee_id     VARCHAR(20) UNIQUE NOT NULL,
    name            VARCHAR(100) NOT NULL,
    department      VARCHAR(50),
    password_hash   TEXT NOT NULL DEFAULT 'admin789',
    is_admin        BOOLEAN DEFAULT FALSE
);

-- Subject ↔ Faculty mapping
CREATE TABLE faculty_subjects (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    faculty_id  UUID NOT NULL REFERENCES faculty(id) ON DELETE CASCADE,
    subject_id  UUID NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
    UNIQUE (faculty_id, subject_id)
);

-- Semester ↔ Subject mapping
CREATE TABLE semester_subjects (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    subject_id  UUID NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
    year        INTEGER NOT NULL CHECK (year BETWEEN 1 AND 4),
    semester    INTEGER NOT NULL CHECK (semester BETWEEN 1 AND 8),
    is_active   BOOLEAN DEFAULT TRUE,
    assigned_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (subject_id, year, semester)
);

-- Timetable periods definition (P1..P8, 8 periods per day)
CREATE TABLE periods (
    id          SERIAL PRIMARY KEY,
    period_num  INTEGER NOT NULL CHECK (period_num BETWEEN 1 AND 8),
    start_time  TIME NOT NULL,
    end_time    TIME NOT NULL,
    label       VARCHAR(20) NOT NULL
);

INSERT INTO periods (period_num, start_time, end_time, label) VALUES
(1, '08:00', '08:50', 'P1'),
(2, '09:20', '10:10', 'P2'),
(3, '10:10', '11:00', 'P3'),
(4, '11:00', '11:50', 'P4'),
(5, '12:40', '13:30', 'P5'),
(6, '13:30', '14:15', 'P6'),
(7, '14:15', '15:00', 'P7'),
(8, '15:15', '16:00', 'P8');

-- ============================================================
-- Faculty Availability (replaces old "slots" table)
-- Each row = one period in a faculty's teaching bundle for a subject
-- All rows sharing the same (faculty_id, subject_id) form a BUNDLE
-- ============================================================
CREATE TABLE faculty_availability (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    faculty_id      UUID NOT NULL REFERENCES faculty(id) ON DELETE CASCADE,
    subject_id      UUID NOT NULL REFERENCES subjects(id) ON DELETE CASCADE,
    stream_name     VARCHAR(50) NOT NULL DEFAULT 'Default Stream', -- e.g., 'Morning Stream', 'Track A'
    day_of_week     INTEGER NOT NULL CHECK (day_of_week BETWEEN 1 AND 5),
    period_id       INTEGER NOT NULL REFERENCES periods(id),
    lecture_seq     INTEGER NOT NULL DEFAULT 1,  -- Session order: 1, 2, 3...
    max_capacity    INTEGER NOT NULL DEFAULT 60,
    current_enrolled INTEGER NOT NULL DEFAULT 0,
    is_active       BOOLEAN DEFAULT TRUE,

    -- Faculty can't be in two places at once in the same stream
    UNIQUE (faculty_id, day_of_week, period_id, stream_name)
);

-- ============================================================
-- Student Selections (references faculty_availability)
-- ============================================================
CREATE TABLE student_selections (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    student_id      UUID NOT NULL REFERENCES students(id) ON DELETE CASCADE,
    availability_id UUID NOT NULL REFERENCES faculty_availability(id) ON DELETE CASCADE,
    is_submitted    BOOLEAN DEFAULT FALSE,
    selected_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (student_id, availability_id)
);

-- Submission lock
CREATE TABLE student_submissions (
    student_id      UUID PRIMARY KEY REFERENCES students(id),
    submitted_at    TIMESTAMPTZ DEFAULT NOW()
);

-- System settings
CREATE TABLE system_settings (
    key   VARCHAR(50) PRIMARY KEY,
    value VARCHAR(255) NOT NULL
);

INSERT INTO system_settings (key, value) VALUES
('student_locked', 'false'),
('faculty_locked', 'false');

-- ============================================================
-- Triggers
-- ============================================================

-- Auto-update enrollment count on insert/delete of student_selections
CREATE OR REPLACE FUNCTION update_enrollment_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE faculty_availability
        SET current_enrolled = current_enrolled + 1
        WHERE id = NEW.availability_id;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE faculty_availability
        SET current_enrolled = current_enrolled - 1
        WHERE id = OLD.availability_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_enrollment
AFTER INSERT OR DELETE ON student_selections
FOR EACH ROW EXECUTE FUNCTION update_enrollment_count();

-- Morning/Evening enforcement: faculty can't teach SAME subject
-- in both morning (P1-P4) and morning again, or evening and evening again
-- But CAN teach once in morning + once in evening on the same day
CREATE OR REPLACE FUNCTION enforce_session_rule()
RETURNS TRIGGER AS $$
DECLARE
    new_session TEXT;
    p_num INTEGER;
    conflict_count INTEGER;
    sub_type TEXT;
    max_allowed INTEGER;
BEGIN
    -- Get period_num for the new row
    SELECT period_num INTO p_num FROM periods WHERE id = NEW.period_id;

    -- Get subject type
    SELECT subject_type INTO sub_type FROM subjects WHERE id = NEW.subject_id;
    
    -- Increase max_allowed to 4 to accommodate Cluster-based scheduling
    max_allowed := 4;

    -- Determine session: morning (P1-P4) or evening (P5-P8)
    new_session := CASE WHEN p_num <= 4 THEN 'morning' ELSE 'evening' END;

    -- Check how many periods this faculty already teaches this subject in the same session
    SELECT COUNT(*) INTO conflict_count
    FROM faculty_availability fa
    JOIN periods p ON p.id = fa.period_id
    WHERE fa.faculty_id = NEW.faculty_id
      AND fa.subject_id = NEW.subject_id
      AND fa.day_of_week = NEW.day_of_week
      AND fa.stream_name = NEW.stream_name
      AND fa.id != COALESCE(NEW.id, uuid_generate_v4())
      AND CASE WHEN p.period_num <= 4 THEN 'morning' ELSE 'evening' END = new_session;

    IF conflict_count >= max_allowed THEN
        RAISE EXCEPTION 'Faculty already reached max periods (%) for this subject in the % session on day %',
            max_allowed, new_session, NEW.day_of_week;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_enforce_session
BEFORE INSERT OR UPDATE ON faculty_availability
FOR EACH ROW EXECUTE FUNCTION enforce_session_rule();

-- ============================================================
-- Indexes
-- ============================================================
CREATE INDEX idx_fa_faculty ON faculty_availability(faculty_id);
CREATE INDEX idx_fa_subject ON faculty_availability(subject_id);
CREATE INDEX idx_fa_day_period ON faculty_availability(day_of_week, period_id);
CREATE INDEX idx_selections_student ON student_selections(student_id);
CREATE INDEX idx_selections_avail ON student_selections(availability_id);
CREATE INDEX idx_selections_submitted ON student_selections(is_submitted);
CREATE INDEX idx_faculty_subjects_subject ON faculty_subjects(subject_id);

-- ============================================================
-- Data Import
-- ============================================================

-- Subjects (with type, periods_per_week, and cluster_id)
INSERT INTO subjects (code, name, credits, subject_type, periods_per_week, cluster_id) VALUES
('HS23211', 'Professional English',              2, 'theory', 2, 1),
('CY23211', 'Engineering Chemistry',             3, 'theory', 3, 1),
('MA23211', 'Statistics and Numerical Methods',  4, 'theory', 4, 2),
('AD23211', 'Python for Data Science',           3, 'theory', 3, 5),
('GE23213', 'Tamils and Technology',             1, 'theory', 1, 3),
('GE23231', 'Engineering Graphics',              4, 'theory', 4, 3),
('AD23231', 'Data Structures Design',            4, 'theory', 4, 4),
('CY23221', 'Chemistry Laboratory',              1, 'lab',    2, 4),
('AD23221', 'Python for Data Science Laboratory', 1, 'lab',   2, 5),
('GE23221', 'Communication Lab / Foreign Language', 1, 'lab', 2, 5);

-- Faculty
INSERT INTO faculty (employee_id, name, department, is_admin) VALUES
('FAC001', 'Dr. Anitha Kumari',       'HSS',        false),
('FAC002', 'Dr. Priya Rajan',         'Chemistry',  false),
('FAC003', 'Prof. Arvind Kumar',      'Mathematics',false),
('FAC004', 'Dr. Meera Suresh',        'CSE',        false),
('FAC005', 'Dr. Karthik Balaji',      'CSE',        false),
('FAC006', 'Prof. Rajesh Venkatesh',  'Civil',      false),
('FAC007', 'Prof. Sunita Mohan',      'CSE',        false),
('FAC008', 'Dr. Lakshmi Narayanan',   'Chemistry',  false),
('FAC009', 'Prof. Divya Shankar',     'CSE',        false),
('FAC010', 'Dr. Ramesh Krishnan',     'HSS',        false),
('FAC011', 'Dr. Kumar Swamy',         'HSS',        false),
('FAC012', 'Prof. Sara Williams',     'HSS',        false),
('FAC013', 'Dr. John Miller',         'Chemistry',  false),
('FAC014', 'Dr. Deepak Rao',          'Chemistry',  false),
('FAC015', 'Prof. Vidya Lakshmi',     'Mathematics',false),
('FAC016', 'Dr. Rahul Sharma',        'CSE',        false),
('FAC017', 'Prof. Shyam Sundar',      'CSE',        false),
('FAC018', 'Dr. Arjun Das',           'CSE',        false),
('FAC019', 'Prof. Manoj Kumar',       'Civil',      false),
('FAC020', 'Dr. Pooja Hegde',         'CSE',        false),
('FAC021', 'Dr. Balu V',              'Chemistry',  false),
('FAC022', 'Prof. Suresh Raina',      'CSE',        false),
('FAC023', 'Dr. Anita Desai',         'HSS',        false),
('ADMIN',  'System Administrator',    'ADMIN',      true);

-- Faculty-Subject Mapping
INSERT INTO faculty_subjects (faculty_id, subject_id)
SELECT f.id, s.id FROM faculty f, subjects s WHERE
  (f.employee_id IN ('FAC001', 'FAC011', 'FAC012') AND s.code='HS23211') OR
  (f.employee_id IN ('FAC002', 'FAC013', 'FAC014') AND s.code='CY23211') OR
  (f.employee_id IN ('FAC003', 'FAC015') AND s.code='MA23211') OR
  (f.employee_id IN ('FAC004', 'FAC016', 'FAC017') AND s.code='AD23211') OR
  (f.employee_id IN ('FAC005', 'FAC018') AND s.code='GE23213') OR
  (f.employee_id IN ('FAC006', 'FAC019') AND s.code='GE23231') OR
  (f.employee_id IN ('FAC007', 'FAC020') AND s.code='AD23231') OR
  (f.employee_id IN ('FAC008', 'FAC021') AND s.code='CY23221') OR
  (f.employee_id IN ('FAC009', 'FAC022') AND s.code='AD23221') OR
  (f.employee_id IN ('FAC010', 'FAC023') AND s.code='GE23221');


-- Students
INSERT INTO students (register_number, name, email, password_hash, department, semester) VALUES
('2117250070001', 'ALICE SMITH', 'alice.smith@university.edu', 'admin789', 'AD', 2),
('2117250070002', 'BOB JOHNSON', 'bob.johnson@university.edu', 'admin789', 'AD', 2),
('2117250070003', 'CHARLIE DAVIS', 'charlie.davis@university.edu', 'admin789', 'AD', 2),
('2117250070004', 'DIANA PRINCE', 'diana.prince@university.edu', 'admin789', 'AD', 2),
('2117250070005', 'EVAN WRIGHT', 'evan.wright@university.edu', 'admin789', 'AD', 2);
