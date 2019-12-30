from sample_operator import MySQLMetadataToAtlasOperator


op = MySQLMetadataToAtlasOperator(
    conn_id='masterdata_prod',
    rdbms_instance_name='aurora_masterdata_prod',
    cloudOrOnPrem='cloud',
    platform='AWS',
    schema='masterdatadb_prod'
)
op.run()