package com.ly.sql.hive;

import org.apache.commons.io.IOUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;


public class WriteOrc {
    public static void main(String[] args) throws Exception {
        // 环境变量
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String createKafkaTableSQL = IOUtils.toString(WriteOrc.class.getResourceAsStream("/kafka_table.sql"));
        String createFsTableSQL = IOUtils.toString(WriteOrc.class.getResourceAsStream("/demo_wuhan_2.sql"));
        String writeOrcSQL = IOUtils.toString(WriteOrc.class.getResourceAsStream("/write_orc.sql"));

    }
}
