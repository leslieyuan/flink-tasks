create table topic_T_NFTTWLL_auto(
  KSPDSJ BIGINT,
  FWSQRYMC STRING,
  SJSWJG_DM STRING,
  YWLX_DM STRING,
  SWJG_DM STRING,
  YWSLRYMC STRING,
  v_database STRING,
  YWLSID STRING,
  FWQDLX_DM STRING,
  KSBLYWSJ BIGINT,
  SJSWJGMC STRING,
  v_table_name STRING,
  v_operation STRING,
  v_timestamp BIGINT,
  SWJGMC STRING,
  JSBLYWSJ BIGINT,
  proctime AS PROCTIME()
) with (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.topic' = 'topic_T_NFTTWLL_auto',
  'connector.startup-mode' = 'latest-offset',
  'connector.properties.0.key' = 'zookeeper.connect',
  'connector.properties.0.value' = '10.101.236.2:2181',
  'connector.properties.1.key' = 'bootstrap.servers',
  'connector.properties.1.value' = '10.101.236.2:6667',
  'connector.properties.2.key' = 'group.id',
  'connector.properties.2.value' = '02020083114310430900000101001496',
  'format.type' = 'json',
  'format.derive-schema' = 'true'
);
create table ElasticSearch001(
  SJSWJG_DM STRING,
  SWJG_DM STRING,
  SJSWJGMC STRING,
  SWJGMC STRING,
  PJDDHS DOUBLE,
  PJBLHS DOUBLE
) with (
  'connector.type' = 'elasticsearch',
  'connector.version' = '6',
  'connector.hosts' = 'http://10.101.232.31:9200',
  'connector.index' = 'pl_test',
  'connector.document-type' = 'pl_test',
  'update-mode' = 'upsert',
  'connector.flush-on-checkpoint' = 'false',
  'connector.bulk-flush.max-actions' = '1',
  'connector.bulk-flush.max-size' = '1 mb',
  'connector.bulk-flush.interval' = '1000',
  'connector.bulk-flush.backoff.max-retries' = '3',
  'connector.bulk-flush.backoff.delay' = '1000',
  'format.type' = 'json'
);
insert into
  ElasticSearch001(SJSWJG_DM, SWJG_DM, SJSWJGMC, SWJGMC, PJDDHS, PJBLHS)
select
  SJSWJG_DM,
  SWJG_DM,
  SJSWJGMC,
  SWJGMC,
  PJDDHS,
  PJBLHS
from
  (
    SELECT
      SJSWJG_DM,
      SWJG_DM,
      SJSWJGMC,
      SWJGMC,
      SUM(KFCKS) AS SUM_KFCKS,
      SUM(KFZDS) AS SUM_KFZDS,
      COUNT(1) AS SLYWS,
      COUNT(DISTINCT FWSQRYMC) AS SLRYS,
      SUM((KSBLYWSJ - KSPDSJ) * 24 * 60) AS PJDDHS,
      SUM((JSBLYWSJ - KSBLYWSJ) * 24 * 60) AS PJBLHS
    FROM
      (
        SELECT
          KSPDSJ,
          FWSQRYMC,
          SJSWJG_DM,
          YWLX_DM,
          SWJG_DM,
          YWSLRYMC,
          v_database,
          YWLSID,
          FWQDLX_DM,
          KSBLYWSJ,
          SJSWJGMC,
          v_table_name,
          v_operation,
          v_timestamp,
          SWJGMC,
          JSBLYWSJ,
          CASE
            when FWQDLX_DM = '01' then 1
            else 0
          end AS KFCKS,
          CASE
            when FWQDLX_DM = '02' then 1
            else 0
          end AS KFZDS
        FROM
          topic_T_NFTTWLL_auto
      ) UDF001
    GROUP BY
      SJSWJG_DM,
      SWJG_DM,
      SJSWJGMC,
      SWJGMC
  )