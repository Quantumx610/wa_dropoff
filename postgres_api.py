import threading
from psycopg2 import pool, sql, OperationalError, InterfaceError
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import os
from dotenv import load_dotenv
# from config import DevelopmentConfig, ProductionConfig 
import logging

logger = logging.getLogger(__name__)

load_dotenv()

env = os.getenv("FLASK_CONFIG")

class PostgresDatabaseAPI:
    _instances = {}
    _lock = threading.Lock()

    _CONFIG_MAP = {
        "uat": "uat",
        "prod": "prod"
    }

    def __new__(cls, db_prefix=env):
        # 1. Resolve which config to use
        config_class = cls._CONFIG_MAP.get(db_prefix.lower())
        if not config_class:
            raise ValueError(f"Invalid DB prefix. Choose from: {list(cls._CONFIG_MAP.keys())}")

        # 2. Use a unique key based on the prefix
        with cls._lock:
            if db_prefix not in cls._instances:
                instance = super().__new__(cls)

                # 3. Pull creds directly from the Config class
                instance.db_pool = pool.ThreadedConnectionPool(
                    minconn=3, maxconn=15,
                    host=config_class.PGRE_HOST,
                    port=config_class.PGRE_PORT,
                    database=config_class.PGRE_DB,
                    user=config_class.PGRE_UNAME,
                    password=config_class.PGRE_PWD
                )
                cls._instances[db_prefix] = instance
                logger.info(f"successfully connected {db_prefix} with new connection")
            else:
                # --- THIS BLOCK RUNS ON EVERY SUBSEQUENT CALL ---
                logger.info(f"Reusing EXISTING connection pool for prefix: {db_prefix}")

        return cls._instances[db_prefix]

    @contextmanager
    def get_cursor(self):
        """
        Production-grade connection handler:
        1. Manages ThreadedConnectionPool safely.
        2. Discards 'poisoned' connections.
        3. Resets transaction state to prevent 'set_session' errors.
        """
        conn = None
        try:
            conn = self.db_pool.getconn()
            
            # --- 1. HEALTH CHECK & SELECTIVE RESET ---
            try:
                # Check if connection is alive
                with conn.cursor() as health_check:
                    health_check.execute("SELECT 1")
                
                # Reset state: Ensure no leftover transactions from previous threads
                conn.rollback() 
                conn.autocommit = True

            except (OperationalError, InterfaceError):
                # Connection is dead; discard it properly and get a fresh one
                logger.warning("Retrieved a dead connection from pool. Discarding and retrying...")
                if conn:
                    self.db_pool.putconn(conn, close=True)
                conn = self.db_pool.getconn()
                conn.autocommit = True

            # --- 2. YIELD THE CURSOR ---
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                yield cursor

        except Exception as e:
            # If any query inside the 'with' block fails, we rollback
            if conn:
                conn.rollback()
            logger.error(f"Database transaction error: {e}", exc_info=True)
            raise e

        finally:
            # --- 3. SAFE RETURN TO POOL ---
            if conn:
                try:
                    self.db_pool.putconn(conn)
                except Exception as e:
                    logger.error(f"Error returning connection to pool: {e}")

    # ------------------------
    # CREATE (INSERT)
    # ------------------------
    def insert(self, table_name, data: dict):
        try:
            with self.get_cursor() as cursor:
                columns = data.keys()
                query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values}) RETURNING *").format(
                    table=sql.Identifier(table_name),
                    fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
                    values=sql.SQL(", ").join(sql.Placeholder() * len(columns))
                )
                cursor.execute(query, list(data.values()))
                return cursor.fetchone()
        except Exception as e:
            print(f"Insert Error: {e}")
            return None

    # ------------------------
    # UPDATE
    # ------------------------
    def update(self, table_name, update_data: dict, filters: dict):
        try:
            with self.get_cursor() as cursor:
                set_items = [sql.SQL("{} = %s").format(sql.Identifier(k)) for k in update_data]
                where_items = [sql.SQL("{} = %s").format(sql.Identifier(k)) for k in filters]

                query = sql.SQL("UPDATE {table} SET {set_clause} WHERE {where_clause} RETURNING *").format(
                    table=sql.Identifier(table_name),
                    set_clause=sql.SQL(", ").join(set_items),
                    where_clause=sql.SQL(" AND ").join(where_items)
                )

                values = list(update_data.values()) + list(filters.values())
                cursor.execute(query, values)
                return cursor.fetchone()
        except Exception as e:
            print(f"Update Error: {e}")
            return None

    # ------------------------
    # READ
    # ------------------------
    def read(self, table_name, columns=None, filters=None):
        try:
            with self.get_cursor() as cursor:
                # 1. Handle Column selection
                col_names = sql.SQL(", ").join(map(sql.Identifier, columns)) if columns else sql.SQL("*")
                query = sql.SQL("SELECT {cols} FROM {table}").format(
                    cols=col_names,
                    table=sql.Identifier(table_name)
                )

                params = []
                if filters:
                    where_items = []
                    for k, v in filters.items():
                        if isinstance(v, dict):
                            op = v.get("op", "=").upper()
                            val = v.get("val")

                            # Handle NULL-safe inequality (Includes NULLs and different values)
                            if op in ("!=", "<>"):
                                # "IS DISTINCT FROM" is Postgres' null-safe inequality operator
                                where_items.append(sql.SQL("{} IS DISTINCT FROM %s").format(sql.Identifier(k)))
                            else:
                                # Generic operator handling (e.g., >, <, >=)
                                # Using sql.SQL for the operator is safe as it's provided by you in code, not user input
                                where_items.append(sql.SQL("{} " + op + " %s").format(sql.Identifier(k)))
                            
                            params.append(val)
                        else:
                            # Existing standard logic: Default to equals
                            where_items.append(sql.SQL("{} = %s").format(sql.Identifier(k)))
                            params.append(v)

                    query += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_items)

                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as e:
            print(f"Read Error: {e}")
            return None

    def update_bulk(self, table_name, update_data: dict, column_name: str, values_list: list):
        """
        Updates multiple rows where column_name is IN values_list.
        Example: UPDATE table SET col1 = val1 WHERE column_name IN (val2, val3)
        """
        try:
            if not values_list:
                return 0

            with self.get_cursor() as cursor:
                # Prepare SET clause: "col1 = %s, col2 = %s"
                set_items = [sql.SQL("{} = %s").format(sql.Identifier(k)) for k in update_data]
                
                # Prepare the query with IN clause
                query = sql.SQL("UPDATE {table} SET {set_clause} WHERE {where_col} IN ({placeholders})").format(
                    table=sql.Identifier(table_name),
                    set_clause=sql.SQL(", ").join(set_items),
                    where_col=sql.Identifier(column_name),
                    # Creates %s, %s, %s based on list length
                    placeholders=sql.SQL(", ").join([sql.Placeholder()] * len(values_list))
                )

                # Combine update values and the list for the IN clause
                query_params = list(update_data.values()) + list(values_list)
                
                cursor.execute(query, query_params)
                return cursor.rowcount  # Returns how many rows were actually updated
        except Exception as e:
            print(f"Bulk Update Error: {e}")
            return None

    def update_bulk_mapped(self, table_name, data_list, id_column='correlation_id'):
        """
        Updates multiple rows with DIFFERENT values for each row in a single query.
        data_list: List of dicts, e.g. [{'correlation_id': 'abc', 'sarvam_attempt_id': '123', 'sarvam_outbound': True}]
        """
        if not data_list:
            return 0

        # 1. Extract all column names from the first dictionary
        all_cols = list(data_list[0].keys())
        # 2. Identify which columns are being updated (exclude the ID/Filter column)
        update_cols = [col for col in all_cols if col != id_column]

        try:
            with self.get_cursor() as cursor:
                # Build the SET clause: "sarvam_attempt_id = v.sarvam_attempt_id, sarvam_outbound = v.sarvam_outbound"
                set_clause = sql.SQL(", ").join([
                    sql.SQL("{col} = v.{col}").format(col=sql.Identifier(col))
                    for col in update_cols
                ])

                # Build the VALUES placeholders: "(%s, %s, %s), (%s, %s, %s)..."
                value_placeholders = sql.SQL(", ").join([
                    sql.SQL("({})").format(sql.SQL(", ").join([sql.Placeholder()] * len(all_cols)))
                    for _ in data_list
                ])

                # Build the alias list for the virtual table 'v'
                v_cols = sql.SQL(", ").join([sql.Identifier(col) for col in all_cols])

                # Final Query Assembly
                query = sql.SQL("""
                    UPDATE {table} AS r
                    SET {set_clause}
                    FROM (VALUES {values_clause}) AS v({v_cols})
                    WHERE r.{id_col} = v.{id_col}
                """).format(
                    table=sql.Identifier(table_name),
                    set_clause=set_clause,
                    values_clause=value_placeholders,
                    v_cols=v_cols,
                    id_col=sql.Identifier(id_column)
                )

                # Flatten all dictionary values into a single list for the execute parameters
                params = []
                for d in data_list:
                    for col in all_cols:
                        params.append(d[col])

                cursor.execute(query, params)
                # self.conn.commit()  # Ensure commit is called if not handled by context manager
                return cursor.rowcount

        except Exception as e:
            print(f"Update Bulk Mapped Error: {e}")
            # self.conn.rollback() 
            return None
        
    def increment_bulk(self, table_name, column_to_inc, where_col, values_list):
        if not values_list: return 0
        with self.get_cursor() as cursor:
            query = sql.SQL("""
                UPDATE {table} 
                SET {inc_col} = COALESCE({inc_col}, 0) + 1 
                WHERE {where_col} IN ({placeholders})
            """).format(
                table=sql.Identifier(table_name),
                inc_col=sql.Identifier(column_to_inc),
                where_col=sql.Identifier(where_col),
                placeholders=sql.SQL(", ").join([sql.Placeholder()] * len(values_list))
            )
            cursor.execute(query, list(values_list))
            return cursor.rowcount

    def execute_raw_query(self, query_string, params=None):
        """
        Executes a raw SQL string. 
        Useful for DELETE, complex JOINs, or DDL commands.
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query_string, params)
                
                # Check if it was a SELECT query to return data
                if cursor.description:
                    return cursor.fetchall()
                
                # Otherwise return the number of rows affected (for DELETE/UPDATE)
                return cursor.rowcount
        except Exception as e:
            logger.error(f"Raw Query Error: {e}")
            print(f"Raw Query Error: {e}")
            return None