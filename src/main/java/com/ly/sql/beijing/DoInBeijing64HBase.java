package com.ly.sql.beijing;

import com.ly.sql.demo.ParseNestedJsonWin;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoInBeijing64HBase {
    private static final Logger log = LoggerFactory.getLogger(ParseNestedJsonWin.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        tableEnv.sqlUpdate("CREATE TABLE person(\n" +
                "  stu_id STRING,\n" +
                "  stu_name STRING,\n" +
                "  score DOUBLE,\n" +
                "  age BIGINT\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'yuanlongtest1',\n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'connector.properties.zookeeper.connect' = 'hdp1.ambari:2181',\n" +
                "  'connector.properties.bootstrap.servers' = 'hdp1.ambari:6667',\n" +
                "  'connector.properties.group.id' = 'consumer1',\n" +
                "  'format.type' = 'json'\n" +
                ")");

        tableEnv.sqlUpdate("CREATE TABLE t_sink (\n" +
                "  rowkey BIGINT,\n" +
                "  f1 ROW<stu_id VARCHAR, stu_name VARCHAR, score DOUBLE, age BIGINT>\n" +
                ") WITH (\n" +
                "  'connector.type' = 'hbase',\n" +
                "  'connector.version' = '1.4.3',\n" +
                "  'connector.table-name' = 't1',\n" +
                "  'connector.zookeeper.quorum' = 'hdp1.ambari:2181', \n" +
                "  'connector.zookeeper.znode.parent' = '/hbase-unsecure',                                                 \n" +
                "  'connector.write.buffer-flush.max-size' = '1mb',\n" +
                "  'connector.write.buffer-flush.max-rows' = '5', \n" +
                "  'connector.write.buffer-flush.interval' = '2s'\n" +
                ")");

        tableEnv.sqlUpdate("INSERT INTO t_sink\n" +
                "SELECT UNIX_TIMESTAMP() AS rowkey, ROW(stu_id , stu_name, score, age) as f1\n" +
                "FROM person");

        tableEnv.execute("test_hbase");
    }
}
