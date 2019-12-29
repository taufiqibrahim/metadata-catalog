from sample_operator import MySQLMetadataToAtlasOperator


op = MySQLMetadataToAtlasOperator(
    conn_id='mysql_sakila',
    rdbms_instance_name='rds_instance_sakila',
    cloudOrOnPrem='cloud',
    platform='Amazon Web Service',
)

op.run()