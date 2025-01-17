CREATE TABLE test_table (
    id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    name NVARCHAR(100) NOT NULL,
    email NVARCHAR(255) NOT NULL,
    date_of_birth DATE,
    timestamp DATETIME2 NOT NULL,
    description TEXT
);
CREATE TABLE test_table_2(
    id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    product_name NVARCHAR(50) NOT NULL,
    product_description NVARCHAR(255) NOT NULL,
    product_price DECIMAL NOT NULL,
    timestamp DATETIME2 NOT NULL
);