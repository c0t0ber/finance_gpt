import logging
import sqlite3
from sqlite3 import Connection

from langchain_community.utilities.sql_database import SQLDatabase

logger = logging.getLogger(__name__)


class TableManager:
    DEFAULT_DIR = "dbs/"
    DRIVER_PREFIX = "sqlite:///"

    @classmethod
    def get_sql_database_tool(cls, name: str) -> SQLDatabase:
        return SQLDatabase.from_uri(cls.DRIVER_PREFIX + cls.get_db_name(name))

    @classmethod
    def get_db_name(cls, name: str) -> str:
        return cls.DEFAULT_DIR + name

    @classmethod
    def crate_connect(cls, name: str) -> Connection:
        return sqlite3.connect(cls.get_db_name(name))

    @classmethod
    def get_last_row(cls, name: str) -> tuple | None:
        conn = cls.crate_connect(name)
        try:
            return conn.execute(
                "select * from expenses order by id desc limit 1"
            ).fetchone()
        except Exception as e:
            logger.exception(e)
        finally:
            conn.close()

    @classmethod
    def init_db(cls, name: str) -> None:
        conn = cls.crate_connect(name)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS expenses (
                    id INTEGER PRIMARY KEY,
                    amount REAL NOT NULL,
                    category TEXT NOT NULL,
                    record_time DATETIME NOT NULL,
                    currency TEXT NOT NULL,
                    description TEXT NOT NULL
                )
            """
            )
            conn.commit()
        except Exception as e:
            logger.exception(e)
        finally:
            if conn:
                conn.close()
