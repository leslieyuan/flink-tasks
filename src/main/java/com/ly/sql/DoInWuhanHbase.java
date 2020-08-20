package com.ly.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DoInWuhanHbase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        tableEnv.executeSql("CREATE TABLE person(\n" +
                "  stu_id STRING,\n" +
                "  stu_name STRING,\n" +
                "  score DOUBLE,\n" +
                "  age BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'yuanlongtest1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'properties.bootstrap.servers' = 'hdp1.ambari:6667',\n" +
                "  'properties.group.id' = 'consumer1',\n" +
                "  'format' = 'json'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE t_sink (\n" +
                "  rowkey BIGINT,\n" +
                "  f1 ROW<stu_id VARCHAR, stu_name VARCHAR, score DOUBLE, age BIGINT>\n" +
                ") WITH (\n" +
                "  'connector' = 'hbase-1.4',\n" +
                "  'table-name' = 't1',\n" +
                "  'zookeeper.quorum' = 'hdp1.ambari:2181', \n" +
                "  'zookeeper.znode.parent' = '/hbase-unsecure',                                                 \n" +
                "  'sink.buffer-flush.max-size' = '1mb',\n" +
                "  'sink.buffer-flush.max-rows' = '5', \n" +
                "  'sink.buffer-flush.interval' = '2s'\n" +
                ")");

        tableEnv.executeSql("INSERT INTO t_sink\n" +
                "SELECT UNIX_TIMESTAMP() AS rowkey, ROW(stu_id , stu_name, score, age) as f1\n" +
                "FROM person");

    }
}
