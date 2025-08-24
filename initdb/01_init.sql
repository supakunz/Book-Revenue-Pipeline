-- สร้าง database
CREATE DATABASE IF NOT EXISTS airflow_db;

-- สร้าง user และให้สิทธิ์
CREATE USER IF NOT EXISTS 'airflow'@'%' IDENTIFIED BY 'airflow';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow'@'%';
FLUSH PRIVILEGES;

-- ใช้ database
USE airflow_db;

-- สร้าง table
CREATE TABLE IF NOT EXISTS data_audible (
    `timestamp` TEXT,
    `user_id` TEXT,
    `book_id` TEXT,
    `country` TEXT,
    `Book Title` TEXT,
    `Book Subtitle` TEXT,
    `Book Author` TEXT,
    `Book Narrator` TEXT,
    `Audio Runtime` TEXT,
    `Audiobook_Type` TEXT,
    `Categories` TEXT,
    `Rating` TEXT,
    `Total No. of Rating` TEXT,
    `Price` TEXT
);
