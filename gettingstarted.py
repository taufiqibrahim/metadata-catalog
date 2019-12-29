import pprint as pp
from atlasclient.client import Atlas

import mysql.connector
sql_get_tables = "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='{DB_NAME}' AND TABLE_TYPE='BASE TABLE';"

conn = mysql.connector.connect(host='localhost',
                               database='sakila',
                               user='tibrahim',
                               password='incorrect')
cur = conn.cursor()
cur.execute(sql_get_tables.format(DB_NAME='sakila'))
records = cur.fetchall()
for record in records:
    print(record)


"""
client = Atlas('localhost', port=21000, username='admin', password='admin')

# params = {'classification': 'Metric'}
# search_results = client.search_dsl(**params)
# for s in search_results:
#     for e in s.entities:
#         print(e.classificationNames)
#         print(e.attributes)


# GUID = '0a6168fe-b76c-4489-b99a-147504abac66'
# GUID = 'bf3df1f0-8f01-439a-8840-2703eb9fa854'
# entity = client.entity_guid(GUID)
# pp.pprint(entity.entity)

INSTANCE_NAME = 'rds_instance_sakila'
ENTITY_INSTANCE = {
    "entity": {
        "typeName": "rdbms_instance",
        "attributes": {
            "name": INSTANCE_NAME,
            "qualifiedName": "rds_instance_sakila@aws",
            "rdbms_type": "aurora-mysql",
            "hostname": "sakila_prod1.aws-aurora.com",
            "port": 3306,
            "cloudOrOnPrem": "cloud",
            "platform": "AWS",
            "owner": "Sakila Product Team"
        }
    }
}
DATABASE_NAME = "sakila_db"
DATABASE_QUALIFIED_NAME = f"{DATABASE_NAME}@{INSTANCE_NAME}"
ENTITY_DATABASE = {
    "entity": {
        "typeName": "rdbms_db",
        "attributes": {
            "name": DATABASE_NAME,
            "qualifiedName": DATABASE_QUALIFIED_NAME,
            "owner": "Sakila Product Team"
        },
        "relationshipAttributes": {
            "instance": {
                "guid": "{{guid}}",
                "typeName": "rdbms_instance"
            }
        }
    }
}

# RDS Instance
res = client.entity_post.create(data=ENTITY_INSTANCE)
instance_guid = [i[1] for i in res['guidAssignments'].items()][0]

# RDS Database
ENTITY_DATABASE["entity"]["relationshipAttributes"]["instance"]["guid"] = instance_guid
res = client.entity_post.create(data=ENTITY_DATABASE)
print(res)
"""
