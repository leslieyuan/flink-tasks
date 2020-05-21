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
    private static final String SOURCE_TOPIC = "";
    private static final String SINK_TOPIC = "";
    private static final Properties KAFKA_PROPERTIES = new Properties();

    static {
        KAFKA_PROPERTIES.setProperty(KafkaConfig.BOOTSTRAP_SERVERS, "192.168.199.102:9092");
        KAFKA_PROPERTIES.setProperty(KafkaConfig.GROUP_ID, "sql");
    }

    private static void main(String[] args) throws Exception {
        // Get the table execute Environment, explicitly set old Planner
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        /* Create a DataStream from Kafka
        *  Trans to a Map stream
        *  Extract col1, col2...
        */
        FlinkKafkaConsumer<String> kfkConsumer = new FlinkKafkaConsumer<String>(SOURCE_TOPIC, new SimpleStringSchema(), KAFKA_PROPERTIES);
        DataStream<String> stringDataStream = env.addSource(kfkConsumer);
        DataStream<Map<String, Object>> mapDataStream = stringDataStream.flatMap(new FlatMapFunction<String, Map<String, Object>>() {
            @Override
            public void flatMap(String s, Collector<Map<String, Object>> collector) throws Exception {
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Object> out = objectMapper.readValue(s, new TypeReference<Map<String, Object>>() {
                });
                // todo: some complex parse
                collector.collect(out);
            }
        });
        DataStream<Tuple2<String, String>> tuple2DataStream = mapDataStream.map(new MapFunction<Map<String, Object>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Map<String, Object> value) throws Exception {
                return new Tuple2<String, String>(value.get("clo1").toString(), value.get("clo2").toString());
            }
        });

        // Convert DataStream to Table
        Table table = tableEnv.fromDataStream(tuple2DataStream, "clo1, clo2");

        // Table sql
        Table resultTable = table
                .filter("clo1 === 'SOME_IGNORED'")
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
