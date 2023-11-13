import sqlite3
from urllib.parse import urlparse

class Initialization:
    def __init__(self):
        self.create_database()

    @ classmethod
    def create_database(cls):
        seed_urls = [
            "https://abadis.ir",
            "https://upmusics.com",
            "https://tiwall.com",
            "https://ershaco.com",
            "https://farsnews.ir",
            "https://filesell.ir",
            "http://pnu.ac.ir",
            "https://varzesh3.com",
            "http://iranseda.ir",
            "https://fa.wikishia.net",
            "https://fa.wikipedia.org",
            "https://ghatreh.com",
            "https://yjc.ir",
            "https://blogfa.com",
            "https://aparat.com",
            "https://digikala.com",
            "https://filimo.com",
            "https://isna.ir"
        ]

        conn = sqlite3.connect('my_database.db')
        # Create a cursor object to execute SQL commands
        cursor = conn.cursor()
        # Loop through the seed URLs and create a table for each
        
        for seed_url in seed_urls:
            netloc = urlparse(seed_url).netloc

            # Sanitize the netloc to create a safe table name
            table_name = ''.join(char for char in netloc if char.isalnum())

            # Define the SQL command to create the table
            create_table_sql = f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                url TEXT,
                depth INTEGER,
                html BLOB,
                extracted_time DATE
            );
            '''
            # Execute the SQL command to create the table
            cursor.execute(create_table_sql)
                
            # Commit the changes and close the connection
            conn.commit()
        conn.close()

        return True
    