package com.ly.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

/**
 * @author yuanlong
 * @version 1.0
 * @description
 * @date 2020/6/12 16:42
 */

public class TestAliasCol {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        tableEnv
                .connect(
                        new Kafka()
                                .version("universal")
                                .topic("ttt")
                                .startFromEarliest()
                                .property("bootstrap.servers", "10.101.232.114:6667")
                                .property("zookeeper.connect", "10.101.232.114:2181")
                                .property("group.id", "MhzSqlDemo")
                )
                .withFormat(new Json()
                        .failOnMissingField(true)
                        .jsonSchema(
                                "{\n" +
                                        "type: 'object',\n" +
                                        "properties: {\n" +
                                        "name:{\n" +
                                        "type:'string'},\n" +
                                        "count:{\n" +
                                        "type:'integer'}}\n" +
                                        "}"
                        ))
                .withSchema(
                        new Schema()
                                .field("name1", DataTypes.STRING()).from("name")
                                .field("count", DataTypes.DECIMAL(38,18))
                )

                .inAppendMode()
                .createTemporaryTable("SourceKafkaTable");

        tableEnv
                .connect(
                        new FileSystem()
                                .path("C:\\Users\\yuanl\\Desktop\\des.txt")
                )
                .withSchema(
                        new Schema()
                                .field("name", DataTypes.STRING())
                                .field("count", DataTypes.DECIMAL(38,18))
                )
                .withFormat(
                        new OldCsv()
                                .field("name", DataTypes.STRING())
                                .field("count", DataTypes.DECIMAL(38,18))
                                .fieldDelimiter(",")
                ).createTemporaryTable("SinkTable");

        tableEnv.sqlUpdate("INSERT INTO SinkTable SELECT name1, `count` FROM SourceKafkaTable");

        tableEnv.execute("kafka_2_file");

    }
}