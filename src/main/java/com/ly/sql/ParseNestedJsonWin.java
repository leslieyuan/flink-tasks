package com.ly.sql;

import com.ly.log4j.Logs;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author yuanlong
 * @version 1.0
 * @description 从kafka解析嵌套json数据，用窗口统计后出发汇总
 * @date 2020/5/29 15:47
 */

public class ParseNestedJsonWin {
    private static final Logger log = LoggerFactory.getLogger(ParseNestedJsonWin.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        tableEnv.sqlUpdate("CREATE TABLE SourceKafkaTable(\n" +
                "    name VARCHAR,\n" +
                "    name1 AS name," +
                "    data ROW<ccount BIGINT, ctimestamp BIGINT>,\n" +
                "    wtime BIGINT,\n" +
                "    ts as TO_TIMESTAMP(FROM_UNIXTIME(wtime /1000,'yyyy-MM-dd HH:mm:ss')),\n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                "    ) WITH (\n" +
                "    -- declare the external system to connect to\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = 'universal',\n" +
                "    'connector.topic' = 't_flinksql',\n" +
                "    'connector.startup-mode' = 'earliest-offset',\n" +
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

//        Table tmp = tableEnv.from("SourceKafkaTable").renameColumns("name as name1");
//        tableEnv.createTemporaryView("tmp", tmp);

        tableEnv.sqlUpdate("CREATE TABLE SinkTable(\n" +
                "window_time TIMESTAMP(3),\n" +
                "name VARCHAR,\n" +
                "`count` BIGINT\n" +
                ") WITH (\n" +
                "-- declare the external system to connect to\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.version' = 'universal',\n" +
                "'connector.topic' = 't_yl_flink_2',\n" +
                "'connector.properties.zookeeper.connect' = '10.101.232.114:2181',\n" +
                "'connector.properties.bootstrap.servers' = '10.101.232.114:6667',\n" +
                "-- specify the update-mode for streaming tables\n" +
                "'update-mode' = 'append',\n" +
                "'connector.sink-partitioner' = 'round-robin',\n" +
                "-- declare a format for this system\n" +
                "'format.type' = 'json',\n" +
                "'format.derive-schema' = 'true'\n" +
                "  )");

//        tableEnv.connect(
//                new FileSystem()
//                        .path("file:///home/yuanlong/des_complex/")
//        )
//                .withSchema(
//                        new Schema()
//                                .field("window_time", TIMESTAMP(3))
//                                .field("name", STRING())
//                                .field("count", DataTypes.BIGINT())
//                )
//                .withFormat(
//                        new OldCsv()
//                                .fieldDelimiter(",")
//                ).createTemporaryTable("SinkTable");

        tableEnv.sqlUpdate("INSERT INTO SinkTable " +
                "SELECT " +
                "TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_time," +
                "name1, SUM(data.ccount) as `count` " +
                "FROM SourceKafkaTable " +
                "GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE),name1");


        try {
            Logs.init("t_yl_flink_sink");
        } catch (IOException ex) {
            log.error("log initial failed.");
        }

        tableEnv.execute("kafka_2_file");
    }

}