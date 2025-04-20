CREATE DATABASE creditdb;
USE creditdb;

CREATE TABLE transactions (
  card_id VARCHAR(255),
  location VARCHAR(255),
  amount FLOAT,
  timestamp DATETIME
);
