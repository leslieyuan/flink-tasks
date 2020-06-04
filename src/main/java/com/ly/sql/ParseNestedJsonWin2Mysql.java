package com.ly.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author yuanlong
 * @version 1.0
 * @description
 * @date 2020/6/4 18:05
 */

public class ParseNestedJsonWin2Mysql {
    private static final Logger log = LoggerFactory.getLogger(ParseNestedJsonWin.class);

    public static void main(String[] args) throws Exception {
        // 环境变量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);e
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        // kafka数据源t_yl_flink
        tableEnv.sqlUpdate("CREATE TABLE SourceKafkaTable(\n" +
                "    name VARCHAR,\n" +
                "    data ROW<ccount BIGINT, ctimestamp BIGINT>,\n" +
                "    wtime BIGINT,\n" +
                "    ts as TO_TIMESTAMP(FROM_UNIXTIME(wtime /1000,'yyyy-MM-dd HH:mm:ss')),\n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                "    ) WITH (\n" +
                "    -- declare the external system to connect to\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = 'universal',\n" +
                "    'connector.topic' = 't_yl_flink',\n" +
                "    'connector.startup-mode' = 'latest-offset',\n" +
                "    'connector.properties.zookeeper.connect' = '10.101.232.114:2181',\n" +
                "    'connector.properties.bootstrap.servers' = '10.101.232.114:6667',\n" +
                "    'connector.properties.group.id' = 'testGroup',\n" +
                "    'connector.properties.key.deserializer' = 'testGroup',\n" +
                "    -- specify the update-mode for streaming tables\n" +
                "    'update-mode' = 'append',\n" +
                "    -- declare a format for this system\n" +
                "    'format.type' = 'json',\n" +
                "    'format.derive-schema' = 'true'\n" +
                "      )");


        tableEnv.sqlUpdate("CREATE TABLE SinkTable(\n" +
                "window_time TIMESTAMP(3),\n" +
                "name VARCHAR,\n" +
                "`count` BIGINT\n" +
                ") WITH (\n" +
                "  'connector.type' = 'jdbc',\n" +
                "  'connector.url' = 'jdbc:mysql://10.101.232.114:3306/flink-test?useUnicode=true&characterEncoding=UTF-8',\n" +
                "  'connector.table' = 'sink_test',\n" +
                "  'connector.username' = 'remote',\n" +
                "  'connector.password' = 'C1stc.0e',\n" +
                "  'connector.lookup.max-retries' = '3',\n" +
                "  'connector.write.flush.interval' = '2s',\n" +
                "  'connector.write.max-retries' = '3'\n" +
                ")");

        tableEnv.sqlUpdate("INSERT INTO SinkTable " +
                "SELECT " +
                "TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_time," +
                "name, SUM(data.ccount) as `count` " +
                "FROM SourceKafkaTable " +
                "GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE),name");


        tableEnv.execute("kafka_2_mysql");
    }
}