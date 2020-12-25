package com.act.app;

import com.act.base.BaseFlinkApp;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class BasicInfoAnalyseApp extends BaseFlinkApp {
    public static void main(String[] args) throws Exception {
        if (args.length < 3 ) {
            System.out.println("usage: BasicInfoAnalyseApp <taskName> <topic> <maxParallelism>");
        }
        String taskName = args[0];
        String topic = args[1];
        String maxParall = args[2];

        BasicInfoAnalyseApp app = new BasicInfoAnalyseApp();
        Configuration conf = app.getFlinkConf(taskName, maxParall);
        StreamExecutionEnvironment bsEnv = new StreamExecutionEnvironment(conf);

        // create kafka stream
        Properties kafkaProperites = app.getkafkaProperties(BasicInfoAnalyseApp.class.getName());
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), kafkaProperites);
        DataStreamSource<String> source = bsEnv.addSource(consumer);

        // flatmap
        SingleOutputStreamOperator<Tuple2<String, Object>> decodedSource = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Object>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Object>> collector) throws Exception {
                        collector.collect(new Tuple2<>(s, null));
                    }
                });

        //

    }
}
