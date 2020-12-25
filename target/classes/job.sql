insert into es_table_ipinfo
 select
  create_time as createTime,
  cast(inform_status as long) as informStatus,
  ip as ip,
  ircs_id as ircsId,
  ircs_name as ircsName,
  rk as `key`
 from hbase_table_ipinfo