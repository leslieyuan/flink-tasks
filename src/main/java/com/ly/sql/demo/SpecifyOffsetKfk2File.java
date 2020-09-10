package com.ly.sql.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author yuanlong
 * @version 1.0
 * @description
 * @date 2020/6/5 13:48
 */

public class SpecifyOffsetKfk2File {
    public static void main(String[] args) throws Exception {
        // 环境变量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        // source file
        tableEnv.sqlUpdate("CREATE TABLE SourceKafkaTable(\n" +
                "name VARCHAR,\n" +
                "data ROW<ccount BIGINT, ctimestamp BIGINT>,\n" +
                "wtime BIGINT,\n" +
                "ts as TO_TIMESTAMP(FROM_UNIXTIME(wtime /1000,'yyyy-MM-dd HH:mm:ss')),\n" +
                "WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "-- declare the external system to connect to\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = 'universal',\n" +
                "'connector.topic' = 't_yl_flink',\n" +
                "'connector.startup-mode' = 'specific-offsets',\n" +
                "'connector.specific-offsets' = 'partition:0,offset:30;partition:1,offset:30',\n" +
                "'connector.properties.zookeeper.connect' = '10.101.232.114:2181',\n" +
                "'connector.properties.bootstrap.servers' = '10.101.232.114:6667',\n" +
                "-- specify the update-mode for streaming tables\n" +
                "'update-mode' = 'append',\n" +
                "-- declare a format for this system\n" +
                "'format.type' = 'json',\n" +
                "'format.derive-schema' = 'true'\n" +
                "  )");

        // destination file
        tableEnv.sqlUpdate("CREATE TABLE SinkTable(\n" +
                "window_time TIMESTAMP(3),\n" +
                "name VARCHAR,\n" +
                "`count` BIGINT\n" +
                ") WITH (\n" +
                "-- declare the external system to connect to\n" +
                "'connector.type' = 'filesystem',\n" +
                "-- specify path\n" +
                "'connector.path' = 'C:\\Users\\yuanl\\Desktop\\destination.txt',\n" +
                "-- specify the update-mode for streaming tables\n" +
                "'update-mode' = 'append',\n" +
                "-- declare a format for this system\n" +
                "'format.type' = 'csv',\n" +
                "'format.derive-schema' = 'true'\n" +
                "  )");

        // doing
        tableEnv.sqlUpdate("INSERT INTO SinkTable " +
                "SELECT " +
                "TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_time," +
                "name, SUM(data.ccount) as `count` " +
                "FROM SourceKafkaTable " +
                "GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE),name");

        // start
        tableEnv.execute("batch_test");

    }
}