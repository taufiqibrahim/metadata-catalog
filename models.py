

class MySQLAtlasEntityTemplate():

    def __init__(self,
                 cloudOrOnPrem,
                 platform,
                 rdbms_instance_name,
                 rdbms_type,
                 rdbms_host,
                 rdbms_port,
                 owner,
                 *args,
                 **kwargs):
        self.cloudOrOnPrem = cloudOrOnPrem
        self.platform = platform
        self.rdbms_instance_name = rdbms_instance_name
        self.rdbms_type = rdbms_type
        self.rdbms_host = rdbms_host
        self.rdbms_port = rdbms_port
        self.owner = owner

    def rdbms_instance(self):
        entity = {
            "entity": {
                "typeName": "rdbms_instance",
                "attributes": {
                    "name": self.rdbms_instance_name,
                    "qualifiedName": "{0}@{1}".format(self.rdbms_instance_name, self.platform),
                    "rdbms_type": self.rdbms_type,
                    "hostname": self.rdbms_host,
                    "port": self.rdbms_port,
                    "cloudOrOnPrem": self.cloudOrOnPrem,
                    "platform": self.platform,
                    "owner": self.owner
                }
            }
        }

        return entity

    def rdbms_db(self, rdbms_instance_guid, db_name):
        entity = {
            "entity": {
                "typeName": "rdbms_db",
                "attributes": {
                    "name": db_name,
                    "qualifiedName": "{0}@{1}".format(db_name, self.rdbms_host),
                    "owner": self.owner
                },
                "relationshipAttributes": {
                    "instance": {
                        "guid": rdbms_instance_guid,
                        "typeName": "rdbms_instance"
                    }
                }
            }
        }

        return entity

    def rdbms_table(self, rdbms_db_guid, table_info):
        table_name = table_info[1]
        qn = "{0}.{1}@{2}".format(table_info[0],
                                  table_info[1],
                                  self.rdbms_host)
        entity = {
            "typeName": "rdbms_table",
            "attributes": {
                "name": table_name,
                "qualifiedName": qn,
                "owner": self.owner
            },
            "relationshipAttributes": {
                "db": {
                    "guid": rdbms_db_guid,
                    "typeName": "rdbms_db"
                }
            }
        }

        return entity

    def rdbms_column(self, rdbms_table_guid, column_info):
        qn = "{0}.{1}.{2}@{3}".format(column_info[0],
                                      column_info[1],
                                      column_info[2],
                                      self.rdbms_host)
        is_primary_key = True if column_info[4] == 1 else False
        is_nullable = True if column_info[5] == 1 else False
        entity = {
            "typeName": "rdbms_column",
            "attributes": {
                "name": column_info[2],
                "qualifiedName": qn,
                "owner": self.owner,
                "data_type": column_info[3],
                "isPrimaryKey": is_primary_key,
                "isNullable": is_nullable
            },
            "relationshipAttributes": {
                "table": {
                    "guid": rdbms_table_guid,
                    "typeName": "rdbms_table"
                }
            }
        }

        return entity
