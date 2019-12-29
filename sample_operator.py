import inspect
import mysql.connector
from atlas_client import driver


class MySQLMetadataToAtlasOperator():

    def __init__(self,
                 conn_id,
                 cloudOrOnPrem,
                 platform,
                 rdbms_instance_name,
                 dry_run=False,
                 *args, **kwargs):
        self.driver = driver
        self.schema = 'sakila'
        self.table_metadata = list()

    def connect(self):
        self.conn = mysql.connector.connect(host='localhost',
                                            database=self.schema,
                                            user='tibrahim',
                                            password='incorrect')
        return self.conn

    def close_connection(self):
        self.conn.close()

    def get_table_metadata(self):
        sql = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='{schema}' AND TABLE_TYPE='BASE TABLE';"
        cur = self.conn.cursor()
        cur.execute(sql.format(schema=self.schema))
        records = cur.fetchall()
        for record in records:
            self.table_metadata.append(record)
        cur.close()

    def get_column_metadata(self):
        pass
    
    def get_columns_metadata(self):
        pass

    def create_or_update_rdbms_instance_metadata(self):
        print('\n>>> ', inspect.stack()[0][3], '...')
        res = client.entity_post.create(data=ENTITY_INSTANCE)

    def create_or_update_rdbms_db_metadata(self, rdbms_instance_guid):
        print('\n>>> ', inspect.stack()[0][3], '...')

    def create_or_update_rdbms_table_metadata(self, rdbms_db_guid):
        print('\n>>> ', inspect.stack()[0][3], '...')
        print(self.table_metadata)

    def create_or_update_rdbms_columns_metadata(self, rdbms_table_guid):
        print('\n>>> ', inspect.stack()[0][3], '...')

    def run(self):
        self.connect()
        self.create_or_update_rdbms_instance_metadata()
        self.create_or_update_rdbms_db_metadata()
        self.get_table_metadata()
        self.create_or_update_rdbms_table_metadata()
        self.get_columns_metadata()
        self.create_or_update_rdbms_columns_metadata()
        self.close_connection()