CREATE TABLE t_nftywll
(
  v_operation  VARCHAR,
  v_timestamp  VARCHAR,
  v_table_name VARCHAR,
  v_database   VARCHAR,
  SJSWJG_DM    VARCHAR,
  SJSWJGMC     VARCHAR,
  SWJG_DM      VARCHAR,
  SWJGMC       VARCHAR,
  FWQDLX_DM    VARCHAR,
  YWLSID       VARCHAR,
  YWSLRYMC     VARCHAR,
  FWSQRYMC     VARCHAR,
  YWLX_DM      VARCHAR,
  KSPDSJ       VARCHAR,
  KSBLYWSJ     VARCHAR,
  JSBLYWSJ     VARCHAR
) WITH
(
  'connector.type' =
  'kafka',
  'connector.version' =
  'universal',
  'connector.topic' =
  't_nftywll_2',
  'connector.startup-mode' =
  'earliest-offset',
  'connector.properties.0.key' =
  'zookeeper.connect',
  'connector.properties.0.value' =
  'hdp1.ambari:2181',
  'connector.properties.1.key' =
  'bootstrap.servers',
  'connector.properties.1.value' =
  'hdp1.ambari:6667',
  'connector.properties.2.key' =
  'group.id',
  'connector.properties.2.value' =
  'test2',
  'update-mode' =
  'append',
  'format.type' =
  'json',
  'format.derive-schema' =
  'true'
);
CREATE TABLE flow_Chart
(
  SJSWJG_DM VARCHAR,
  SJSWJGMC  VARCHAR,
  SWJG_DM   VARCHAR,
  SWJGMC    VARCHAR
) WITH
(
  'connector.type' =
  'jdbc',
  'connector.url' =
  'jdbc:mysql://10.101.232.114:3306/flink-test?useUnicode=true&characterEncoding=utf-8',
  'connector.table' =
  'flow_Chart',
  'connector.username' =
  'remote',
  'connector.password' =
  'C1stc.0e',
  'connector.write.flush.max-rows' =
  '1'
);
INSERT INTO flow_Chart
SELECT SJSWJG_DM,SJSWJGMC,SWJG_DM,SWJGMC
FROM t_nftywll "


##################################### my test #########################################################################

CREATE TABLE SourceKafkaTable(
name VARCHAR,
data ROW<ccount BIGINT, ctimestamp BIGINT>,
wtime BIGINT,
ts as TO_TIMESTAMP(FROM_UNIXTIME(wtime /1000,'yyyy-MM-dd HH:mm:ss')),
WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
-- declare the external system to connect to
'connector.type' = 'kafka',
'connector.version' = 'universal',
'connector.topic' = 't_yl_flink',
'connector.startup-mode' = 'latest-offset',
'connector.properties.zookeeper.connect' = '10.101.232.114:2181',
'connector.properties.bootstrap.servers' = '10.101.232.114:6667',
-- specify the update-mode for streaming tables
'update-mode' = 'append',
-- declare a format for this system
'format.type' = 'json',
'format.derive-schema' = 'true'
  )
















