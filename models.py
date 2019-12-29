

class RDBMSAtlasModelTemplate():

    def __init__(self,
                 cloudOrOnPrem,
                 platform,
                 rdbms_instance_name,
                 rdbms_port,
                 *args,
                 **kwargs):
        self.rdbms_instance_name = rdbms_instance_name

    def RDBMSInstance(self):
        ENTITY_INSTANCE = {
            "entity": {
                "typeName": "rdbms_instance",
                "attributes": {
                    "name": self.rdbms_instance_name,
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

# class MySQLAtlasModel()
