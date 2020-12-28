package com.act.app;

import com.act.base.BaseFlinkApp;
import com.act.entity.BasicInfo;
import com.act.entity.LogFile;
import com.act.entity.Result;
import com.act.flatmap.BasicInfoFlatMap;
import com.act.flatmap.LogFileFlatMap;
import com.act.flatmap.ValidateAndDecodeFlatMap;
import com.act.sink.jdbc.BasicInfoSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.File;
import java.util.Properties;

@Slf4j
public class BasicInfoAnalyseApp extends BaseFlinkApp {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("usage: BasicInfoAnalyseApp <taskName> <topic> <maxParallelism>");
        }
        String taskName = args[0];
        String topic = args[1];
        String maxParall = args[2];

        BasicInfoAnalyseApp app = new BasicInfoAnalyseApp();
        Configuration conf = app.getFlinkConf(taskName, maxParall);
        StreamExecutionEnvironment bsEnv = new StreamExecutionEnvironment(conf);

        // set global parametes
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String configPath = parameterTool.get("configFile");
        if (System.getProperties().getProperty("os.name").toUpperCase().contains("WINDOWS")) {
            configPath = "conf/global.properties";
        }
        File file = new File(configPath);
        ParameterTool parameter = ParameterTool.fromPropertiesFile(file);
        bsEnv.getConfig().setGlobalJobParameters(parameter);

        // create kafka stream
        Properties kafkaProperites = app.getkafkaProperties(BasicInfoAnalyseApp.class.getName());
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), kafkaProperites);
        consumer.setStartFromGroupOffsets();
        DataStreamSource<String> source = bsEnv.addSource(consumer);

        // LogFile flatmap
        DataStream<Tuple2<LogFile, Result>> logFileStream = source.flatMap(new LogFileFlatMap()).name("2logFile");

        // decode
        DataStream<Tuple2<String, Result>> decodedStream = logFileStream.flatMap(new ValidateAndDecodeFlatMap()).name("decode");

        // BasicInfo flatmap
        DataStream<Tuple2<BasicInfo, Result>> basicInfoStream = decodedStream.flatMap(new BasicInfoFlatMap()).name("2basicInfo");

        // split basic info stream
        final OutputTag<Tuple2<BasicInfo, Result>> insertTag = new OutputTag<>("insert_data");
        final OutputTag<Tuple2<BasicInfo, Result>> updateTag = new OutputTag<>("update_data");
        final OutputTag<Tuple2<BasicInfo, Result>> deleteTag = new OutputTag<>("delete_data");
        final OutputTag<Tuple2<BasicInfo, Result>> queryTag = new OutputTag<>("query_data");
        SingleOutputStreamOperator<Tuple2<BasicInfo, Result>> mainStream = basicInfoStream.process(
                new ProcessFunction<Tuple2<BasicInfo, Result>, Tuple2<BasicInfo, Result>>() {
                    @Override
                    public void processElement(
                            Tuple2<BasicInfo, Result> basicInfoResultTuple2,
                            Context context,
                            Collector<Tuple2<BasicInfo, Result>> collector) throws Exception {
                        collector.collect(basicInfoResultTuple2);

                        BasicInfo basicInfo = basicInfoResultTuple2.f0;
                        NewInfo newInfo = basicInfo.getNewInfo();
                        UpdateInfo updateInfo = basicInfo.getUpdateInfo();
                        QueryResult queryResult = basicInfo.getQueryResult();
                        DeleteInfo deleteInfo = basicInfo.getDeleteInfo();
                        if (newInfo != null) {
                            context.output(insertTag, basicInfoResultTuple2);
                        }
                        if (updateInfo != null) {
                            context.output(updateTag, basicInfoResultTuple2);
                        }
                        if (queryResult != null) {
                            context.output(queryTag, basicInfoResultTuple2);
                        }
                        if (deleteInfo != null) {
                            context.output(deleteTag, basicInfoResultTuple2);
                        }
                    }
                }
        ).name("split_stream");
        DataStream<Tuple2<BasicInfo, Result>> insertStream = mainStream.getSideOutput(insertTag);
        DataStream<Tuple2<BasicInfo, Result>> updateStream = mainStream.getSideOutput(updateTag);
        DataStream<Tuple2<BasicInfo, Result>> deleteStream = mainStream.getSideOutput(deleteTag);
        DataStream<Tuple2<BasicInfo, Result>> queryStream = mainStream.getSideOutput(queryTag);

        // deal stream
        insertStream.addSink(new BasicInfoSink());

        // start
        bsEnv.execute(taskName);
    }
}
