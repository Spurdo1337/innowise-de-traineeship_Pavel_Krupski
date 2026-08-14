-- Schema for the dormitory (rooms / students) database.
-- Relationship: many students -> one room (many-to-one via students.room_id).

CREATE TABLE IF NOT EXISTS rooms (
    id   INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS students (
    id       INTEGER PRIMARY KEY,
    name     VARCHAR(255) NOT NULL,
    birthday DATE NOT NULL,
    sex      CHAR(1) NOT NULL CHECK (sex IN ('M', 'F')),
    room_id  INTEGER NOT NULL REFERENCES rooms(id)
);
