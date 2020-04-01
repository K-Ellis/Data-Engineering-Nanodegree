CREATE DATABASE studentdb;
CREATE USER student with encrypted password 'student';
GRANT ALL PRIVILEGES ON DATABASE studentdb TO student;
ALTER USER student CREATEDB;
