package com.act.base;

import com.act.util.KafkaProperties;
import com.ly.util.TransUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.util.Properties;

@Slf4j
public abstract class BaseFlinkApp {
    private static Properties properties;

    static {
        InputStream inputStream = BaseFlinkApp.class.getClassLoader().getResourceAsStream("global.properties");
        try {
            properties.load(inputStream);
        } catch (Exception e) {
            log.error("Initial global map failed.");
        }

    }

    public Properties getkafkaProperties(String groupId) {
        return KafkaProperties.getKafkaProperties(properties.getProperty("kafka.url"), groupId);
    }

    public Configuration getFlinkConf(String taskName, String maxParall) {
        Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.NAME, taskName);
        configuration.set(PipelineOptions.GLOBAL_JOB_PARAMETERS, TransUtil.pro2Map(properties));
        configuration.set(PipelineOptions.MAX_PARALLELISM, Integer.parseInt(maxParall));
        configuration.set(PipelineOptions.OPERATOR_CHAINING, true);
        return configuration;
    }

    public void setFlinkEnv() {

    }

    public void run(Configuration conf, String topic, String groupId) {


    }


}
