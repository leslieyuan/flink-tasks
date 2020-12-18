CREATE TABLE hbase_table_ipinfo (
  rowkey STRING,
  a ROW<create_time STRING, ip STRING, ip_standard_info_id STRING, ircs_id STRING, ircs_name STRING,
    infor_status STRING, province_id STRING, data_source_num STRING, visit_count STRING, last_time STRING>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'hbase-1.4',
  'table-name' = 'mytable',
  'zookeeper.quorum' = 'localhost:2181'
)