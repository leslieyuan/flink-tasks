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
 * @description 从kafka解析复杂josn格式，写入其他地方
 * @date 2020/5/29 14:02
 */

public class ParseNestedJson {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        tableEnv
                .connect(
                        new Kafka()
                                .version("universal")
                                .topic("t_yl_flink")
                                .startFromLatest()
                                .property("bootstrap.servers", "10.101.232.114:6667")
                                .property("zookeeper.connect", "10.101.232.114:2181")
                                .property("group.id", "sqldemo")
                )
                .withSchema(
                        new Schema()
                                .field("name", STRING())
                                .field("data", ROW(FIELD("ccount", BIGINT()),
                                        FIELD("ctimestamp", BIGINT())))
                )
                .withFormat(new Json()
                                .failOnMissingField(true)
                )
                .inAppendMode()
                .createTemporaryTable("SourceKafkaTable");

        tableEnv
                .connect(
                        new FileSystem()
                                .path("C:\\Users\\yuanl\\Desktop\\des_complex")
                )
                .withSchema(
                        new Schema()
                                .field("name", STRING())
                                .field("count", DataTypes.DECIMAL(38, 2))
                )
                .withFormat(
                        new OldCsv()
                                .fieldDelimiter(",")
                ).createTemporaryTable("SinkTable");

        tableEnv.sqlUpdate("INSERT INTO SinkTable(name, `count`) SELECT name, data.ccount FROM SourceKafkaTable");

        tableEnv.execute("kafka_2_file");
    }
}