package com.ly;

import com.ly.util.ReadStringFromFile;
import com.ly.util.TransUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class Hbase2EsApp {
    public static void main(String[] args) throws Exception {
        // 环境变量
        Properties properties = new Properties();
        properties.load(Hbase2EsApp.class.getClassLoader().getResourceAsStream("global.properties"));

        Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.NAME, "my_task_name");
        configuration.set(PipelineOptions.GLOBAL_JOB_PARAMETERS, TransUtil.pro2Map(properties));
        configuration.set(PipelineOptions.MAX_PARALLELISM, 10);
        configuration.set(PipelineOptions.OPERATOR_CHAINING, true);

        StreamExecutionEnvironment bsEnv = new StreamExecutionEnvironment(configuration);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        // do
        bsTableEnv.executeSql(ReadStringFromFile.read("hbase.sql"));
        bsTableEnv.executeSql(ReadStringFromFile.read("es.sql"));
        bsTableEnv.executeSql(ReadStringFromFile.read("job.sql"));
    }
}
