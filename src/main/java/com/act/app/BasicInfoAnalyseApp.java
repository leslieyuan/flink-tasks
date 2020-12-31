package com.act.app;

import com.act.base.BaseFlinkApp;
import com.act.service.BasicInfoAnalyseService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class BasicInfoAnalyseApp extends BaseFlinkApp {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (parameterTool.getNumberOfParameters() < 3) {
            System.out.println("usage: BasicInfoAnalyseApp --task-name <taskName> --max-parall <maxParall> " +
                    "--topic <topic> --config-path <path>");
            System.exit(0);
        }
        String taskName = parameterTool.get("task-name");
        String maxParall = parameterTool.get("max-parall");
        String topic = parameterTool.get("topic");
        String configPath = parameterTool.get("config-path");
        if (System.getProperties().getProperty("os.name").toUpperCase().contains("WINDOWS")) {
            configPath = "conf/global.properties";
        }

        BasicInfoAnalyseApp app = new BasicInfoAnalyseApp();
        app.execute(taskName, maxParall, topic, BasicInfoAnalyseApp.class.getName(), configPath);
    }

    @Override
    public void handleStream(StreamExecutionEnvironment env, DataStreamSource<String> source) {
        BasicInfoAnalyseService.handle(source);
    }
}
