import inspect
import mysql.connector
import pprint as pp
from atlas_client import driver
from atlasclient.utils import parse_table_qualified_name
from models import MySQLAtlasEntityTemplate
from requests import Timeout


class MySQLMetadataToAtlasOperator():

    def __init__(self,
                 conn_id,
                 cloudOrOnPrem,
                 platform,
                 rdbms_instance_name,
                 schema,
                 owner='bizzy',
                 dry_run=False,
                 *args, **kwargs):
        self.driver = driver
        self.schema = schema
        self.table_metadata = list()
        self.table_columns_metadata = list()

        self.rdbms_type = 'mysql'
        self.rdbms_host = 'db-mdm-prod-ro.tokosmart.id'
        self.rdbms_port = 3306

        self.model = MySQLAtlasEntityTemplate(cloudOrOnPrem=cloudOrOnPrem,
                                              platform=platform,
                                              rdbms_instance_name=rdbms_instance_name,
                                              rdbms_type=self.rdbms_type,
                                              rdbms_host=self.rdbms_host,
                                              rdbms_port=self.rdbms_port,
                                              owner=owner)

    def connect(self):
        self.conn = mysql.connector.connect(host=self.rdbms_host,
                                            database=self.schema,
                                            user='ro_datalake',
                                            password='20d@TaL4Ke456')
        return self.conn

    def close_connection(self):
        self.conn.close()

    def get_table_metadata(self):
        sql = "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='{schema}' AND TABLE_TYPE='BASE TABLE';"
        cur = self.conn.cursor()
        cur.execute(sql.format(schema=self.schema))
        records = cur.fetchall()
        for record in records:
            self.table_metadata.append(record)
        cur.close()

    def get_guid_from_response(self, res):
        return [i[1] for i in res['guidAssignments'].items()][0]

    def get_column_metadata(self):
        pass

    def get_columns_metadata(self, table_schema, table_name):
        table_columns_metadata = list()
        sql = """SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
        CASE WHEN COLUMN_KEY = 'PRI' THEN TRUE ELSE FALSE END AS is_primary_key,
        CASE WHEN IS_NULLABLE = 'NO' THEN FALSE ELSE TRUE END AS is_nullable
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA='{table_schema}' AND TABLE_NAME='{table_name}';
        """
        cur = self.conn.cursor()
        cur.execute(sql.format(table_schema=table_schema, table_name=table_name))
        records = cur.fetchall()
        for record in records:
            table_columns_metadata.append(record)
        cur.close()

        return table_columns_metadata

    def create_entities(self, entities_to_create):
        try:
            driver.entity_bulk.create(data={"entities": entities_to_create})
        except Timeout as ex:
            # Try one more time in case of Timeout error!!
            print(f'ReadTimeout : {ex}')
            driver.entity_bulk.create(data={"entities": entities_to_create})

    def create_or_update_rdbms_instance_metadata(self):
        print('\n>>> ', inspect.stack()[0][3], '...')
        res = self.driver.entity_post.create(data=self.model.rdbms_instance())
        self.rdbms_instance_guid = self.get_guid_from_response(res)

    def create_or_update_rdbms_db_metadata(self):
        print('\n>>> ', inspect.stack()[0][3], '...')
        data = self.model.rdbms_db(
            rdbms_instance_guid=self.rdbms_instance_guid,
            db_name = self.schema
        )
        res = self.driver.entity_post.create(data=data)
        self.rdbms_db_guid = self.get_guid_from_response(res)

    def create_or_update_rdbms_table_metadata(self):
        print('\n>>> ', inspect.stack()[0][3], '...')
        entities_to_create = list()

        for t in self.table_metadata:
            table_info = (t[0], t[1], )
            data = self.model.rdbms_table(self.rdbms_db_guid, table_info)
            entities_to_create.append(data)
        
        if entities_to_create:
            self.create_entities(entities_to_create)

    def create_or_update_rdbms_columns_metadata(self):
        print('\n>>> ', inspect.stack()[0][3], '...')
        
        for t in self.table_metadata:
            entities_to_create = list()
            table_qn = "{0}.{1}@{2}".format(t[0], t[1], self.rdbms_host)
            table_guid = self.driver.entity_unique_attribute('rdbms_table', qualifiedName=table_qn).entity['guid']
            
            column_info = self.get_columns_metadata(table_schema=t[0], table_name=t[1])

            for c in column_info:
                data = self.model.rdbms_column(table_guid, c)
                entities_to_create.append(data)
            
            if entities_to_create:
                self.create_entities(entities_to_create)

    def run(self):
        self.connect()
        self.create_or_update_rdbms_instance_metadata()
        self.create_or_update_rdbms_db_metadata()
        self.get_table_metadata()
        self.create_or_update_rdbms_table_metadata()
        self.create_or_update_rdbms_columns_metadata()
        self.close_connection()
