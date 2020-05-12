package com.ly.play;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWordCnt {
    private static final String IP = "192.168.199.102";
    public static void main(String[] args) throws Exception {
        // The port to connect
        final int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("need set a port");
            return;
        }

        // Env set
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> source = env.socketTextStream(IP, port, "\n");

        DataStream<Tuple2<String, Integer>> wordCntStream = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.trim().replaceAll("\\s+", " ").split(" ");
                for (String w : words) {
                    out.collect(new Tuple2<String, Integer>(w, 1));
                }
            }
        });
        wordCntStream.keyBy(0).timeWindow(Time.seconds(5)).sum(1).print();

        // Start
        env.execute("word count");
    }
}
