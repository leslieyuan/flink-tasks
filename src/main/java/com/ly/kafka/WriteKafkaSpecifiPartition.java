package com.ly.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaContextAware;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class WriteKafkaSpecifiPartition {
    private static final String SOURCE_TOPIC = "work1";
    private static final String SINK_TOPIC = "yuanlongtest";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.101.236.2:6667");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                SOURCE_TOPIC,
                new SimpleStringSchema(),
                properties
        );
        consumer.setStartFromEarliest();
        DataStream<String> source = environment.addSource(consumer);

        // sink
        Properties properties1 = new Properties();
        properties1.setProperty("bootstrap.servers", "10.101.236.2:6667");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
                SINK_TOPIC,
                new MyKafkaSerializationSchema(SINK_TOPIC),
                properties1,
                FlinkKafkaProducer.Semantic.NONE);
        source.addSink(producer);

        environment.execute("job");
    }

    /** How to serialization and send to which partition. **/
    static class MyKafkaSerializationSchema implements KafkaSerializationSchema<String>, KafkaContextAware {
        private String topic;
        private Integer partition;

        MyKafkaSerializationSchema(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
            try {
                Thread.sleep(123);
            } catch (Exception e) {
                e.printStackTrace();
            }

            return new ProducerRecord<byte[], byte[]>(
                    this.topic,
                    this.partition,
                    timestamp,
                    String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8),
                    element.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String getTargetTopic(Object element) {
            return this.topic;
        }

        @Override
        public void setPartitions(int[] partitions) {
            // hash send to partition
            this.partition = (int) (System.currentTimeMillis() % partitions.length);
            System.out.println("write to partition " + this.partition);
        }
    }
}
