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
  'connector.properties.2.value' = '02020101010394624000000101001310',
  'format.type' = 'json',
  'format.derive-schema' = 'true'
);
create table T_NFTYWLL_copy1(
  KSPDSJ BIGINT,
  FWSQRYMC STRING,
  SJSWJG_DM STRING,
  YWLX_DM STRING,
  SWJG_DM STRING,
  YWSLRYMC STRING,
  YWLSID STRING,
  FWQDLX_DM STRING,
  KSBLYWSJ BIGINT,
  SJSWJGMC STRING,
  SWJGMC STRING,
  JSBLYWSJ BIGINT
) with (
  'connector.type' = 'jdbc',
  'connector.url' = 'jdbc:mysql://10.101.236.3:3306/binlog_test?useUnicode=true&characterEncoding=UTF-8',
  'connector.table' = 'T_NFTYWLL_copy1',
  'connector.username' = 'u_binlogtest',
  'connector.password' = 'yhn.C2l*>3(Qp',
  'connector.write.flush.max-rows' = '1'
);
insert into
  T_NFTYWLL_copy1(
    KSPDSJ,
    FWSQRYMC,
    SJSWJG_DM,
    YWLX_DM,
    SWJG_DM,
    YWSLRYMC,
    YWLSID,
    FWQDLX_DM,
    KSBLYWSJ,
    SJSWJGMC,
    SWJGMC,
    JSBLYWSJ
  )
select
  KSPDSJ,
  FWSQRYMC,
  SJSWJG_DM,
  YWLX_DM,
  SWJG_DM,
  YWSLRYMC,
  YWLSID,
  FWQDLX_DM,
  KSBLYWSJ,
  SJSWJGMC,
  SWJGMC,
  JSBLYWSJ
from
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
      JSBLYWSJ
    FROM
      topic_T_NFTTWLL_auto
    order by
      SWJG_DM ASC
  );