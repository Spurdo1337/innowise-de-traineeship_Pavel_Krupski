-- Indexes required to optimize the reporting queries.
--
-- Why each index exists:
--
-- 1. idx_students_room_id
--    Speeds up the JOIN/GROUP BY between students and rooms that every
--    single report query performs (students.room_id -> rooms.id).
--    Without it, every query does a full sequential scan of "students".
--
-- 2. idx_students_room_id_birthday
--    A composite (room_id, birthday) index lets PostgreSQL compute
--    AVG/MIN/MAX(birthday) per room using an index-only / index scan
--    instead of reading every row of the students table. This directly
--    speeds up "5 rooms with smallest average age" and
--    "5 rooms with the largest age difference".
--
-- 3. idx_students_room_id_sex
--    A composite (room_id, sex) index speeds up the
--    "rooms with different-sex students" query, which needs
--    COUNT(DISTINCT sex) per room_id.
--
-- Indexes are created AFTER the bulk data load (see src/main.py), which
-- is the standard best practice: building an index once over already
-- loaded data is much faster than maintaining it row-by-row during
-- thousands of INSERTs.

CREATE INDEX IF NOT EXISTS idx_students_room_id
    ON students (room_id);

CREATE INDEX IF NOT EXISTS idx_students_room_id_birthday
    ON students (room_id, birthday);

CREATE INDEX IF NOT EXISTS idx_students_room_id_sex
    ON students (room_id, sex);
