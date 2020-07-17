from configparser import ConfigParser
from DwhLoad.db_pool import CursorFromConnectionFromPool, Database
import os


#create schemas and tables
parser = ConfigParser()
parser.read('config.ini')

Database.initialize(database = parser.get('Redshift', 'database_name'),
                    user = parser.get('Redshift', 'username'),
                    password = os.environ['RS_PASSWORD'],
                    port = parser.get('Redshift', 'port'),
                    host = parser.get('Redshift', 'url')
                    )

def create_mrr_tables():
    with CursorFromConnectionFromPool() as cursor:
        with open('resources/mrr_init.sql') as f:
            cursor.execute(f.read())


def create_dwh_tables():
    with CursorFromConnectionFromPool() as cursor:
        with open('resources/dwh_init.sql') as f:
            cursor.execute(f.read())
