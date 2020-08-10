package com.ly.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoInBeijing64ES {
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

        tableEnv.sqlUpdate("CREATE TABLE stuTable (\n" +
                "  stu_id STRING,\n" +
                "  stu_name STRING,\n" +
                "  score DOUBLE,\n" +
                "  age BIGINT\n" +
                ") WITH (\n" +
                "  'connector.type' = 'elasticsearch', \n" +
                "  'connector.version' = '6',\n" +
                "  'connector.hosts' = 'http://hdp1.ambari:9200/',\n" +
                "  'connector.index' = 'my-test',       \n" +
                "  'connector.document-type' = 'user',    \n" +
                "  'update-mode' = 'append',    \n" +
                "  'connector.flush-on-checkpoint' = 'false', \n" +
                "  'connector.bulk-flush.max-actions' = '1',\n" +
                "  'connector.bulk-flush.max-size' = '1 mb',\n" +
                "  'connector.bulk-flush.interval' = '1', \n" +
                "  'connector.bulk-flush.backoff.max-retries' = '3',\n" +
                "  'connector.bulk-flush.backoff.delay' = '1000',\n" +
                "  'format.type' = 'json'\n" +
                ")");

        tableEnv.sqlUpdate("INSERT INTO stuTable\n" +
                "SELECT stu_id , stu_name, score, age\n" +
                "FROM person");

        tableEnv.execute("test");
    }
}
