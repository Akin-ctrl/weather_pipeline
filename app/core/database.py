"""
Database manager with connection pooling and context management.
================================================================
Handles all database operations with proper resource management.
"""

import logging
from typing import Optional, Any, Generator
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor


logger = logging.getLogger(__name__)


class DatabaseConnectionError(Exception):
    """Raised when database connection fails."""
    pass


class DatabaseManager:
    """
    Manages PostgreSQL database connections with pooling.
    
    Provides connection pooling for efficient resource usage and
    context managers for safe connection handling.
    
    Attributes:
        _host: Database host address.
        _database: Database name.
        _user: Database username.
        _password: Database password.
        _pool: Connection pool instance.
    
    Example:
        >>> db = DatabaseManager(host="localhost", database="weather", 
        ...                       user="user", password="pass")
        >>> with db.get_connection() as conn:
        ...     with conn.cursor() as cur:
        ...         cur.execute("SELECT * FROM cities")
        ...         results = cur.fetchall()
    """
    
    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        min_connections: int = 1,
        max_connections: int = 10
    ) -> None:
        """
        Initialize database manager with connection pooling.
        
        Args:
            host: Database host address.
            database: Database name.
            user: Database username.
            password: Database password.
            min_connections: Minimum pool size.
            max_connections: Maximum pool size.
        
        Raises:
            DatabaseConnectionError: If initial connection fails.
        """
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        
        try:
            self._pool = psycopg2.pool.SimpleConnectionPool(
                min_connections,
                max_connections,
                host=host,
                database=database,
                user=user,
                password=password
            )
            logger.info(f"Database connection pool created (min={min_connections}, max={max_connections})")
        except psycopg2.Error as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise DatabaseConnectionError(f"Could not connect to database: {e}")
    
    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """
        Get a database connection from the pool.
        
        Yields:
            Database connection object.
        
        Raises:
            DatabaseConnectionError: If no connections available.
        
        Example:
            >>> with db.get_connection() as conn:
            ...     cur = conn.cursor()
            ...     cur.execute("SELECT 1")
        """
        conn = None
        try:
            conn = self._pool.getconn()
            if conn is None:
                raise DatabaseConnectionError("No connection available from pool")
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if conn:
                self._pool.putconn(conn)
    
    def execute_query(
        self,
        query: str,
        params: Optional[tuple] = None,
        fetch_one: bool = False,
        fetch_all: bool = True
    ) -> Optional[Any]:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query string.
            params: Query parameters tuple.
            fetch_one: Return single row if True.
            fetch_all: Return all rows if True (ignored if fetch_one=True).
        
        Returns:
            Query results or None for non-SELECT queries.
        
        Raises:
            DatabaseConnectionError: If query execution fails.
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                try:
                    cur.execute(query, params or ())
                    
                    if fetch_one:
                        return cur.fetchone()
                    elif fetch_all and cur.description:
                        return cur.fetchall()
                    return None
                except psycopg2.Error as e:
                    logger.error(f"Query execution failed: {e}")
                    raise DatabaseConnectionError(f"Query failed: {e}")
    
    def execute_many(self, query: str, params_list: list[tuple]) -> None:
        """
        Execute a query multiple times with different parameters.
        
        Args:
            query: SQL query string.
            params_list: List of parameter tuples.
        
        Raises:
            DatabaseConnectionError: If batch execution fails.
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.executemany(query, params_list)
                    logger.info(f"Executed batch query with {len(params_list)} parameter sets")
                except psycopg2.Error as e:
                    logger.error(f"Batch execution failed: {e}")
                    raise DatabaseConnectionError(f"Batch query failed: {e}")
    
    def close(self) -> None:
        """Close all connections in the pool."""
        if hasattr(self, '_pool') and self._pool:
            self._pool.closeall()
            logger.info("All database connections closed")
    
    def __enter__(self):
        """Support context manager protocol."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up on context manager exit."""
        self.close()
    
    def __repr__(self) -> str:
        return f"DatabaseManager(host='{self._host}', database='{self._database}')"
