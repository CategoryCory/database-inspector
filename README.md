# Database Inspector

A flexible Python library for inspecting various types of SQL databases. This tool provides a unified interface to 
examine database structure, including tables and columns, across different database management systems.

## Supported Databases

- SQLite
- MySQL
- PostgreSQL
- Microsoft SQL Server

## Features

- Connection management for multiple database types
- Table listing
- Column inspection with detailed information:
  - Column names
  - Data types (automatically mapped to Python types)
  - Nullable status
- Connection status monitoring
- Consistent error handling across database types

## Installation

To install the required dependencies:

- MySQL:
  ```bash
  uv add --group mysql mysql-connector-python
  ```
- PostgreSQL:
  ```bash
  uv add --group postgres "psycopg[binary]"
  ```
- Microsoft SQL Server:
  ```bash
  uv add --group mssql pyodbc
  ```

## Usage Examples

### SQLite Database

```python
from database_inspector.db.sqlite_db_connection import SqliteDbConnection

connection_params = "path/to/database.db" 
with SqliteDbConnection(connection_params) as db: 
    # Get all tables
    tables = db.get_tables()
    # Get columns for a specific table
    columns = db.get_columns("table_name")
```

### MySQL Database

```python
from database_inspector.db.mysql_db_connection import MySqlDbConnection
from database_inspector.infrastructure.models import ConnectionParams

params = ConnectionParams(
    host="localhost",
    port=5432,
    database="your_database",
    user="your_username",
    password="your_password"
)
with MySqlDbConnection(params) as db: 
    # Get all tables
    tables = db.get_tables()
    
    # Get columns for a specific table
    columns = db.get_columns("table_name")
```

### PostgreSQL Database

```python
from database_inspector.db.postgres_db_connection import PostgresDbConnection
from database_inspector.infrastructure.models import ConnectionParams

params = ConnectionParams(
    host="localhost",
    port=5432,
    database="your_database",
    user="your_username",
    password="your_password"
)
with PostgresDbConnection(params) as db: 
    # Get all tables
    tables = db.get_tables()
    
    # Get columns for a specific table
    columns = db.get_columns("table_name")
```

### Microsoft SQL Server

```python
from database_inspector.db.mssql_db_connection import MSSqlDbConnection
from database_inspector.infrastructure.models import ConnectionParams

# Connect to MS SQL Server
params = ConnectionParams(
    host="localhost",
    port=1433,
    database="your_database",
    user="your_username",
    password="your_password"
)

with MSSqlDbConnection(params) as db:
    # Get all tables
    tables = db.get_tables()
    
    # Get columns for a specific table
    columns = db.get_columns("table_name")
```

## Error Handling

The library provides consistent error handling across all database types with specific exceptions:

- `DatabaseConnectionError`: Raised when database connection fails
- `DatabaseTableNotFoundError`: Raised when attempting to access a non-existent table

## Testing

### Prerequisites

Before running tests, ensure you have:

1. Docker installed and running on your machine
2. Python 3.13 or higher
3. Test dependencies installed:
   ```bash
   uv add --group test pytest pytest-cov testcontainers
   ```

### Running Tests

To run the full test suite:

```bash
pytest
```

To run tests with coverage report:

```bash
pytest --cov=database_inspector
```

### Test Environment

The test suite uses `testcontainers` to spin up Docker containers for testing database connections. This ensures:

- Tests run against real database instances
- Consistent test environment across different development machines
- No need to manually set up test databases
- Clean state for each test run

The following containers are used during testing:
- PostgreSQL
- Microsoft SQL Server
- SQLite (file-based, no container needed)

### Writing Tests

When contributing new features, please ensure:

1. All tests are placed in the `tests/` directory
2. Test files follow the naming pattern `test_*.py`
3. Each test function name starts with `test_`
4. Test classes follow the naming pattern `Test*`
5. Use fixtures where appropriate to reduce code duplication
6. Include both positive and negative test cases
7. Mock external dependencies when appropriate

### Troubleshooting Tests

If tests fail due to Docker-related issues:

1. Ensure Docker daemon is running
2. Check if required ports are available (PostgreSQL: 5432, MSSQL: 1433)
3. Verify sufficient system resources for container deployment
4. Check Docker logs for potential container startup issues

## Requirements

- Python 3.13 or higher
- psycopg (for PostgreSQL)
- pyodbc (for Microsoft SQL Server)
- sqlite3 (built into Python, for SQLite)

## License

MIT License

## Contributing

We welcome contributions to the Database Inspector project! Here's how you can help:

### Getting Started

1. Fork the repository
2. Create a new branch for your feature or bug fix:
   ```bash
   git checkout -b feature/your-feature-name
   ```
   or
   ```bash
   git checkout -b fix/your-bug-fix
   ```

### Development Setup

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install development dependencies:
   ```bash
   uv add mypy pylint pytest
   ```

### Coding Standards

- Follow PEP 8 style guide
- Maintain type hints throughout the codebase
- Keep cyclomatic complexity low
- Write docstrings for all public methods and classes
- Maintain 100% test coverage for new code

### Pull Request Process

1. Update the documentation if needed
2. Add tests for new features
3. Ensure all tests pass
4. Update the README.md if needed
5. Create a Pull Request with a clear description of the changes

### Reporting Issues

When reporting issues, please include:

- A clear description of the problem
- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Database type and version
- Python version
- Stack trace if applicable

### Feature Requests

We welcome feature requests! Please provide:

- A clear description of the feature
- Use cases for the feature
- Any potential implementation details you have in mind

### Code of Conduct

- Be respectful and inclusive
- Provide constructive feedback
- Focus on what is best for the community
- Show empathy towards other community members
