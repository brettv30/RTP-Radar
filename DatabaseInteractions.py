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

    def connect_to_postgres(self):
        """Connect to PostgreSQL server"""
        with timer("Connecting to PostgreSQL server"):
            try:
                config = self.load_config()
                # connecting to the PostgreSQL server
                with psycopg2.connect(**config) as conn:
                    logger.info("Connected to PostgreSQL server.")
                    return conn
            except (psycopg2.DatabaseError, Exception) as error:
                logger.error(error)


class DatabaseManipulate(DatabaseConfig):
    def __init__(self, file, section) -> None:
        super().__init__(file, section)

    def run_commands(self, commands):
        with timer("Running database command(s)"):
            try:
                conn = self.connect_to_postgres()
                if conn is not None:
                    with conn.cursor() as cur:
                        for command in commands:
                            cur.execute(command)
                        conn.commit()
                    logger.info("Command executed successfully.")
            except (psycopg2.DatabaseError, Exception) as error:
                logger.error(error)
            finally:
                if conn is not None:
                    conn.close()


class NoSectionError(Exception):
    def __init__(self, message):
        super().__init__(message)


if __name__ == "__main__":
    create_landing_table_command = [
        """
        CREATE TABLE  land_tbl_raw_feeds(
            table_id integer primary key generated always as identity,
            extraction_date timestamp with time zone not null,
            published_date timestamp with time zone not null,
            url text not null, 
            author text, 
            title text not null,
            content text not null
        )
        """
    ]
    pg_server = DatabaseManipulate("database.ini", "postgresql")
    pg_server.run_commands(create_landing_table_command)
