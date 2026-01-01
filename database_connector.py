from collections.abc import Generator
import duckdb
import pandas as pd



class DatabaseError(Exception):
    """Exception raised for database-related errors."""
    pass


class DatabaseConnector:
    """A class to connect to a DuckDB database and execute queries."""

    def __init__(self, db_path=":memory:"):
        """Initializes the DatabaseConnector.

        Args:
            db_path (str, optional): The path to the DuckDB database file.
                Defaults to ":memory:", which creates an in-memory database.
        """
        self.db_path = db_path
        self.conn = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Connects to the DuckDB database.

        Returns:
            duckdb.DuckDBPyConnection: A connection object to the database.

        Raises:
            DatabaseError: If the connection fails.
        """
        if not self.conn:
            try:
                self.conn = duckdb.connect(self.db_path)
                # Install and load spatial extension for geospatial operations
                self._setup_spatial_extension()
            except Exception as e:
                print(f"Failed to connect to database at '{self.db_path}': {e}")
                raise DatabaseError(f"Database connection failed: {e}") from e
        return self.conn

    def _setup_spatial_extension(self) -> None:
        """Install and load the DuckDB spatial extension for geospatial operations."""
        try:
            # Install spatial extension if not already installed
            self.conn.execute("INSTALL spatial;")
            # Load spatial extension
            self.conn.execute("LOAD spatial;")
        except Exception as e:
            # Extension might already be installed/loaded, which is fine
            print(f"Spatial extension setup: {e}")
            pass

    def execute(self, query, params=None) -> list:
        """Executes a SQL query and fetches all results.

        Args:
            query (str): The SQL query to execute.
            params (list, optional): A list of parameters to substitute into the query.
                Defaults to None.

        Returns:
            list: A list of tuples representing the query results.

        Raises:
            DatabaseError: If the query execution fails.
        """
        try:
            conn = self.connect()
            if params:
                return conn.execute(query, params).fetchall()
            return conn.execute(query).fetchall()
        except Exception as e:
            if isinstance(e, DatabaseError):
                raise
            print(f"Database query execution failed: {e}")
            raise DatabaseError(f"Query execution failed: {e}") from e

    def execute_df(self, query, params=None) -> pd.DataFrame:
        """Executes a SQL query and returns the results as a Pandas DataFrame.

        Args:
            query (str): The SQL query to execute.
            params (list, optional): A list of parameters to substitute into the query.
                Defaults to None.

        Returns:
            pandas.DataFrame: A DataFrame containing the query results.

        Raises:
            DatabaseError: If the query execution fails.
        """
        try:
            conn = self.connect()
            if params:
                return conn.execute(query, params).df()
            return conn.execute(query).df()
        except Exception as e:
            if isinstance(e, DatabaseError):
                raise
            print(f"Database query execution failed: {e}", )
            raise DatabaseError(f"Query execution failed: {e}") from e

    def close(self) -> None:
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None


def get_db() -> Generator[DatabaseConnector, None, None]:
    db = DatabaseConnector("transit.duckdb")
    try:
        db.connect()
        yield db
    finally:
        db.close()
