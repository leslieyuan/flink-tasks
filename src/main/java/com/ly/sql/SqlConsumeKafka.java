package com.ly.sql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

public class SqlConsumeKafka {
    private static final Logger log = LoggerFactory.getLogger(SqlConsumeKafka.class);
    private static final String SOURCE_TOPIC = "t_yl_flink";
    private static final String SINK_TOPIC = "t_yl_flink_sink";
    private static final Properties KAFKA_PROPERTIES = new Properties();

    static {
        KAFKA_PROPERTIES.setProperty(KafkaConfig.BOOTSTRAP_SERVERS, "10.101.232.114:6667");
        KAFKA_PROPERTIES.setProperty(KafkaConfig.GROUP_ID, "yl_test");
    }

    public static void main(String[] args) throws Exception {
        // Get the table execute Environment, explicitly set old Planner
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        /* Create a DataStream from Kafka
        *  Trans to a Map stream
        *  Extract col1, col2...
        */
        FlinkKafkaConsumer<String> kfkConsumer = new FlinkKafkaConsumer<String>(SOURCE_TOPIC, new SimpleStringSchema(), KAFKA_PROPERTIES);
        kfkConsumer.setStartFromLatest();
        DataStream<String> stringDataStream = env.addSource(kfkConsumer);
        DataStream<UserTime> pojoDataStream = stringDataStream.flatMap(new FlatMapFunction<String, UserTime>() {
            @Override
            public void flatMap(String s, Collector<UserTime> collector) throws Exception {
                ObjectMapper objectMapper = new ObjectMapper();
                UserTime out = objectMapper.readValue(s, new TypeReference<UserTime>() {
                });
                // todo: some complex parse
                collector.collect(out);
            }
        });
        DataStream<Tuple2<String, Long>> tuple2DataStream = pojoDataStream.flatMap(new FlatMapFunction<UserTime, Tuple2<String, Long>>() {
            @Override
            public void flatMap(UserTime value, Collector<Tuple2<String, Long>> out) throws Exception {
                out.collect(Tuple2.of(value.getUser(), value.getTime()));
            }
        });

        // Convert DataStream to Table
        Table table = tableEnv.fromDataStream(tuple2DataStream, "clo1, clo2");
        // Register Table view Test
//        tableEnv.createTemporaryView("Test", tuple2DataStream, "clo1, clo2");

        // Table sql
//        Table resultTable = tableEnv.sqlQuery(
//                "SELECT clo1, clo2 FROM " + table + " WHERE clo1 == 'SOME_IGNORED'");
        // Table api
        Table resultTable = table
                .filter("clo1 === 'yl'")
                .select("clo1, clo2");

        /* Result table to DataStream
         * Sink to kafka
         */
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(resultTable, Row.class);
        rowDataStream.addSink(new FlinkKafkaProducer<Row>(SINK_TOPIC, new KafkaSerializationSchema<Row>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(Row row, @Nullable Long aLong) {
                return new ProducerRecord<>(SINK_TOPIC, null, row.toString().getBytes(StandardCharsets.UTF_8));
            }
        }, KAFKA_PROPERTIES, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        // Start
        env.execute("table api test");
    }
}
