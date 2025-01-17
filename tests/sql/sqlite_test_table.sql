CREATE TABLE test_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    date_of_birth DATE,
    timestamp DATETIME NOT NULL,
    description TEXT
);
CREATE TABLE test_table_2 (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    product_name TEXT NOT NULL,
    product_description TEXT NOT NULL,
    product_price REAL NOT NULL,
    timestamp DATETIME NOT NULL
);