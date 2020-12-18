package com.ly;

import com.ly.util.ReadStringFromFile;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Hbase2EsApp {
    public static void main(String[] args) {
        // 环境变量
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        // do
        bsTableEnv.executeSql(ReadStringFromFile.read("hbase.sql"));
        bsTableEnv.executeSql(ReadStringFromFile.read("es.sql"));
        bsTableEnv.executeSql(ReadStringFromFile.read("job.sql"));
    }
}
