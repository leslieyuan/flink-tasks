CREATE TABLE t_nftywll(
  v_operation VARCHAR,
  v_timestamp VARCHAR,
  v_table_name VARCHAR,
  v_database VARCHAR,
  SJSWJG_DM VARCHAR,
  SJSWJGMC VARCHAR,
  SWJG_DM VARCHAR,
  SWJGMC VARCHAR,
  FWQDLX_DM VARCHAR,
  YWLSID VARCHAR,
  YWSLRYMC VARCHAR,
  FWSQRYMC VARCHAR,
  YWLX_DM VARCHAR,
  KSPDSJ BIGINT,
  KSBLYWSJ BIGINT,
  JSBLYWSJ BIGINT
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.topic' = '0826heheda',
  'connector.startup-mode' = 'latest-offset',
  'connector.properties.0.key' = 'zookeeper.connect',
  'connector.properties.0.value' = 'hdp1.ambari:2181',
  'connector.properties.1.key' = 'bootstrap.servers',
  'connector.properties.1.value' = 'hdp1.ambari:6667',
  'connector.properties.2.key' = 'group.id',
  'connector.properties.2.value' = 'test9',
  'update-mode' = 'append',
  'format.type' = 'json',
  'format.derive-schema' = 'true'
);
CREATE TABLE T_YY_ZHDP_SWDJZTJK_BSTHXZL_LJS_yuanlong(
  SJSWJG_DM VARCHAR,
  SJSWJGMC VARCHAR,
  SWJG_DM VARCHAR,
  SWJGMC VARCHAR,
  KFCKS DOUBLE,
  KFZDS DOUBLE,
  SLYWS DOUBLE,
  SLRYS DOUBLE,
  PJDDHS DOUBLE,
  PJBLHS DOUBLE
) WITH (
  'connector.type' = 'jdbc',
  'connector.url' = 'jdbc:mysql://10.101.236.3:3306/binlog_test?useUnicode=true&characterEncoding=UTF-8',
  'connector.table' = 'T_YY_ZHDP_SWDJZTJK_BSTHXZL_LJS_yuanlong',
  'connector.username' = 'remote',
  'connector.password' = 'C1stc.0e',
  'connector.lookup.max-retries' = '3',
  'connector.write.flush.max-rows' = '500',
  'connector.write.flush.interval' = '2s',
  'connector.write.max-retries' = '3'
);
INSERT INTO
  T_YY_ZHDP_SWDJZTJK_BSTHXZL_LJS_yuanlong
SELECT
  SJSWJG_DM,
  SJSWJGMC,
  SWJG_DM,
  SWJGMC,
  SUM(
    CASE
      when FWQDLX_DM = '01' then 1
      else 0
    end
  ) KFCKS,
  SUM(
    CASE
      when FWQDLX_DM = '02' then 1
      else 0
    end
  ) KFZDS,
  COUNT(1) SLYWS,
  COUNT(DISTINCT FWSQRYMC) SLRYS,
  ROUND(SUM((KSBLYWSJ - KSPDSJ) * 24 * 60), 2) PJDDHS,
  ROUND(SUM((JSBLYWSJ - KSBLYWSJ) * 24 * 60), 2) PJBLHS
FROM
  t_nftywll
GROUP BY
  SJSWJG_DM,
  SJSWJGMC,
  SWJG_DM,
  SWJGMC