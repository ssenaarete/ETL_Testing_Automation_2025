import pyodbc
import logging
import configparser

class DBHelper:
    def __init__(self, server, database, driver, username=None, password=None):
        self.server = server
        self.database = database
        self.driver = driver
        self.username = username
        self.password = password
        self.conn = None

    @classmethod
    # def from_config(cls, config_path,section_name):
    def from_config_section(cls, config_path,section_name):    
        """Load DB config from a config.ini file."""
        config = configparser.ConfigParser()
        config.read(config_path)

        # server = config.get("SOURCEDB", "server")
        # database = config.get("SOURCEDB", "database")
        # driver = config.get("SOURCEDB", "driver")
        # username = config.get("SOURCEDB", "username", fallback="").strip() or None
        # password = config.get("SOURCEDB", "password", fallback="").strip() or None
        server = config.get(section_name, "server")
        database = config.get(section_name, "database")
        driver = config.get(section_name, "driver")
        username = config.get(section_name, "username", fallback="").strip() or None
        password = config.get(section_name, "password", fallback="").strip() or None


        return cls(server, database, driver, username, password)

    def connect(self):
        try:
            if self.username and self.password:
                # SQL Authentication
                conn_str = (
                    f"DRIVER={self.driver};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password}"
                )
            else:
                # Windows Authentication
                conn_str = (
                    f"DRIVER={self.driver};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"Trusted_Connection=yes;"
                )

            self.conn = pyodbc.connect(conn_str)
            logging.info("✅ Database connection established.")
        except Exception as e:
            logging.error(f"❌ Error connecting to database: {e}")
            raise

    def execute_query(self, query):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            # row = cursor.fetchone()
            row = cursor.fetchall()
            # if row:
            #     return row[0]  # Return the first column value
            # else:
            #     return None  # Or 0, or raise exception as per your need
            
            result = [tuple(r) for r in row]
            return result
    
            # return row  # return everything
        except Exception as e:
            logging.error(f"❌ Error executing query '{query}': {e}")
            raise
        

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("🔒 Database connection closed.")
