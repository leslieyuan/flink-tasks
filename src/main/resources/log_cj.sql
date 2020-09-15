create table t_kafka_flink_log(
  rwId STRING,
  rwLxDm STRING,
  sj STRING,
  nr STRING
) with (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.topic' = 't_donline_rw_log_topic_flink',
  'connector.startup-mode' = 'latest-offset',
  'connector.properties.0.key' = 'zookeeper.connect',
  'connector.properties.0.value' = '10.101.236.2:2181',
  'connector.properties.1.key' = 'bootstrap.servers',
  'connector.properties.1.value' = '10.101.236.2:6667',
  'connector.properties.2.key' = 'group.id',
  'connector.properties.2.value' = 'k_2_es',
  'format.type' = 'json',
  'format.derive-schema' = 'true'
);
create table t_es_flink_log(
  rwid STRING,
  rz STRING,
  yxsj STRING,
  rzsj BIGINT,
  zt STRING,
  rzjb STRING
) with (
  'connector.type' = 'elasticsearch',
  'connector.version' = '6',
  'connector.hosts' = 'http://10.101.232.31:9200',
  'connector.index' = 'cj_rw_log',
  'connector.document-type' = '_doc',
  'update-mode' = 'append',
  'connector.flush-on-checkpoint' = 'false',
  'connector.bulk-flush.max-actions' = '1000',
  'connector.bulk-flush.max-size' = '2 mb',
  'connector.bulk-flush.interval' = '1000',
  'connector.bulk-flush.backoff.max-retries' = '3',
  'connector.bulk-flush.backoff.delay' = '1000',
  'format.type' = 'json'
);
INSERT INTO
  t_es_flink_log(rwid, rz, yxsj, rzsj, zt, rzjb)
SELECT
  rwId AS rwid,
  nr AS rz,
  sj AS yxsj,
  UNIX_TIMESTAMP(sj) * 1000 AS rzsj,
  CASE
    WHEN POSITION('FAILED' IN nr) <> 0 THEN 'FAILED'
    WHEN POSITION('CANCEL' IN nr) <> 0 THEN 'CANCEL'
    WHEN POSITION('RUNNING' IN nr) <> 0 THEN 'RUNNING'
    ELSE 'RUNNING'
  END AS zt,
  CASE
    WHEN POSITION('INFO' IN nr) <> 0 THEN 'INFO'
    WHEN POSITION('ERROR' IN nr) <> 0 THEN 'ERROR'
    WHEN POSITION('WARN' IN nr) <> 0 THEN 'WARN'
    WHEN POSITION('DEBUG' IN nr) <> 0 THEN 'DEBUG'
    ELSE 'no_level'
  END AS rzjb
FROM t_kafka_flink_log