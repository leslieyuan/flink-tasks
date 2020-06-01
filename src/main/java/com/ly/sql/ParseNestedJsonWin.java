package com.ly.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

import static org.apache.flink.table.api.DataTypes.*;

/**
 * @author yuanlong
 * @version 1.0
 * @description 从kafka解析嵌套json数据，用窗口统计后出发汇总
 * @date 2020/5/29 15:47
 */

public class ParseNestedJsonWin {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

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
                "    -- specify the update-mode for streaming tables\n" +
                "    'update-mode' = 'append',\n" +
                "    -- declare a format for this system\n" +
                "    'format.type' = 'json',\n" +
                "    'format.derive-schema' = 'true'\n" +
                "      )");

        tableEnv
                .connect(
                        new FileSystem()
                                .path("C:\\Users\\yuanl\\Desktop\\des_complex")
                )
                .withSchema(
                        new Schema()
                                .field("window_time", TIMESTAMP(3))
                                .field("name", STRING())
                                .field("count", DataTypes.BIGINT())
                )
                .withFormat(
                        new OldCsv()
                                .fieldDelimiter(",")
                ).createTemporaryTable("SinkTable");

        tableEnv.sqlUpdate("INSERT INTO SinkTable " +
                "SELECT " +
                "TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_time," +
                "name, SUM(data.ccount) as `count` " +
                "FROM SourceKafkaTable " +
                "GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE),name");

        tableEnv.execute("kafka_2_file");
    }

}