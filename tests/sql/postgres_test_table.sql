CREATE TABLE test_table (
    id SERIAL PRIMARY KEY NOT NULL,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    date_of_birth DATE,
    timestamp TIMESTAMP NOT NULL,
    description TEXT
);
CREATE TABLE test_table_2(
    id SERIAL PRIMARY KEY NOT NULL,
    product_name VARCHAR(50) NOT NULL,
    product_description VARCHAR(255) NOT NULL,
    product_price DECIMAL NOT NULL,
    timestamp TIMESTAMP NOT NULL
);