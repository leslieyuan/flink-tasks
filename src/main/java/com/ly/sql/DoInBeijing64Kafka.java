package com.ly.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoInBeijing64Kafka {
    private static final Logger log = LoggerFactory.getLogger(ParseNestedJsonWin.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        tableEnv.sqlUpdate("CREATE TABLE t_from(\n" +
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
                "  'connector.properties.group.id' = 'consumer2',\n" +
                "  'format.type' = 'json'\n" +
                ")");

        tableEnv.sqlUpdate("CREATE TABLE t_sink(\n" +
                "  stu_id STRING,\n" +
                "  stu_name STRING,\n" +
                "  score DOUBLE,\n" +
                "  age BIGINT\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'yuanlongtest2',\n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'connector.properties.zookeeper.connect' = 'hdp1.ambari:2181',\n" +
                "  'connector.properties.bootstrap.servers' = 'hdp1.ambari:6667',\n" +
                "  'format.type' = 'json'\n" +
                ")");

        tableEnv.sqlUpdate("INSERT INTO t_sink\n" +
                "SELECT stu_id , stu_name, score, age\n" +
                "FROM t_from");

        tableEnv.execute("test");
    }
}
