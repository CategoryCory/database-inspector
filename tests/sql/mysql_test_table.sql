CREATE TABLE test_table (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    date_of_birth DATE,
    timestamp DATETIME NOT NULL,
    description TEXT
);
CREATE TABLE test_table_2 (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(50) NOT NULL,
    product_description VARCHAR(255) NOT NULL,
    product_price DECIMAL NOT NULL,
    timestamp DATETIME NOT NULL
);
