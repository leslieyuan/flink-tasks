CREATE TABLE es_table_ipinfo (
  createTime STRING,
  informStatus STRING,
  ip STRING,
  ircsId STRING,
  ircsName STRING,
  `key` STRING,
  PRIMARY KEY (`key`) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://localhost:9200',
  'index' = 'users',
  'username' = '',
  'password' = ''
  'sink.bulk-flush.max-actions' = '1000',
  'sink.bulk-flush.max-size' = '2mb',
  'sink.bulk-flush.interval' = '1s',
  'sink.bulk-flush.interval' = '1s',
);