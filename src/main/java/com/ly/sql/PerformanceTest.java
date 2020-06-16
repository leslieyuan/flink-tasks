package com.ly.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author yuanlong
 * @version 1.0
 * @description
 * @date 2020/6/15 16:25
 */

public class PerformanceTest {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        tableEnv.sqlUpdate("CREATE TABLE source_info(\n" +
                "user_id VARCHAR,\n" +
                "item_id VARCHAR,\n" +
                "category_id VARCHAR ,\n" +
                "behavior VARCHAR ,\n" +
                "ts VARCHAR\n" +
                ") WITH (\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = 'universal',\n" +
                "'connector.topic' = 'xingneng2',\n" +
                "'connector.properties.group.id' = 'performance',\n" +
                "'connector.startup-mode' = 'earliest-offset',\n" +
                "'connector.properties.zookeeper.connect' = '10.101.236.2:2181',\n" +
                "'connector.properties.bootstrap.servers' = '10.101.236.2:6667',\n" +
                "'update-mode' = 'append',\n" +
                "'format.type' = 'json',\n" +
                "'format.derive-schema' = 'true'\n" +
                "  )");

        tableEnv.sqlUpdate("CREATE TABLE behavior_info(\n" +
                "user_id VARCHAR ,\n" +
                "item_id VARCHAR ,\n" +
                "category_id VARCHAR ,\n" +
                "behavior VARCHAR\n" +
                ") WITH (\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = 'universal',\n" +
                "'connector.topic' = 'xingneng',\n" +
                "'connector.properties.zookeeper.connect' = '10.101.236.2:2181',\n" +
                "'connector.properties.bootstrap.servers' = '10.101.236.2:6667',\n" +
                "'update-mode' = 'append',\n" +
                "'format.type' = 'json',\n" +
                "'format.derive-schema' = 'true'\n" +
                "  )");

        tableEnv.sqlUpdate("INSERT INTO behavior_info SELECT user_id, item_id, category_id, behavior FROM source_info");

        tableEnv.execute("performance_test");
    }
}