from configparser import ConfigParser
import psycopg2
from contextlib import contextmanager
import time as tme
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@contextmanager
def timer(label):
    start = tme.time()
    try:
        yield
    finally:
        end = tme.time()
    time = round(end - start, 2)
    logger.info(f"{label}: {time} seconds")


class DatabaseConfig:
    def __init__(self, filename, section) -> None:
        self.filename = filename
        self.section = section

    def load_config(self):
        with timer(f"Loading {self.filename}"):
            parser = ConfigParser()
            parser.read(self.filename)

            if not parser.has_section(self.section):
                raise NoSectionError(
                    "Section {0} not found in the {1} file".format(
                        self.section, self.filename
                    )
                )

            params = parser.items(self.section)
        return {param[0]: param[1] for param in params}

    def connect_to_postgres(self, config):
        """Connect to the PostgreSQL database server"""
        with timer("Connecting to the PostgreSQL database"):
            try:
                # connecting to the PostgreSQL server
                with psycopg2.connect(**config) as conn:
                    logger.info("Connected to the PostgreSQL server.")
                    return conn
            except (psycopg2.DatabaseError, Exception) as error:
                logger.error(error)


class DatabaseCreate:
    def __init__(self, connection) -> None:
        self.connection = connection

    def create_database(self, database_name):
        """Create a new database"""
        with timer(f"Creating a new database - {database_name}"):
            with self.connection.cursor() as cur:
                # create a new database
                cur.execute("CREATE DATABASE xxxxxxx")
                logger.info("Database created successfully.")

    def create_tables(self, commands):
        """Create tables in the PostgreSQL database"""
        with timer("Creating tables"):
            try:
                with self.connection.cursor() as cur:
                    # execute the CREATE TABLE statement
                    for command in commands:
                        cur.execute(command)
            except (psycopg2.DatabaseError, Exception) as error:
                print(error)


class DatabaseInsert:
    def __init__(self, db_config) -> None:
        self.db_config = db_config


class DatabaseExtract:
    def __init__(self, db_config) -> None:
        self.db_config = db_config


class NoSectionError(Exception):
    def __init__(self, message):
        super().__init__(message)


if __name__ == "__main__":
    pg_config = DatabaseConfig("database.ini", "postgresql")
    config = pg_config.load_config()
    conn = pg_config.connect_to_postgres(config)
