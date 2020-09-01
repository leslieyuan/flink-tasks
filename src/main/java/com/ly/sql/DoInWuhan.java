package com.ly.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class DoInWuhan {
    public static void main(String[] args) throws Exception {
        // 环境变量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        String jobName = (args.length == 0 ? "no name" : args[0]);

        tableEnv.sqlUpdate("create\n" +
                "\ttable\n" +
                "\t\twork1(\n" +
                "\t\t\tFWSQRYMC VARCHAR\n" +
                "\t\t) with(\n" +
                "\t\t\t'connector.type' = 'kafka',\n" +
                "\t\t\t'connector.version' = 'universal',\n" +
                "\t\t\t'connector.topic' = 'xingneng',\n" +
                "\t\t\t'connector.startup-mode' = 'earliest-offset',\n" +
                "\t\t\t'connector.properties.0.key' = 'zookeeper.connect',\n" +
                "\t\t\t'connector.properties.0.value' = '10.101.236.2:2181',\n" +
                "\t\t\t'connector.properties.1.key' = 'bootstrap.servers',\n" +
                "\t\t\t'connector.properties.1.value' = '10.101.236.2:6667',\n" +
                "\t\t\t'connector.properties.2.key' = 'group.id',\n" +
                "\t\t\t'connector.properties.2.value' = '02020062216021101400000101001103',\n" +
                "\t\t\t'format.type' = 'json',\n" +
                "\t\t\t'format.derive-schema' = 'true'\n" +
                "\t\t)");

        tableEnv.sqlUpdate("create\n" +
                "\ttable\n" +
                "\t\tschool(\n" +
                "\t\t\tid varchar\n" +
                "\t\t) WITH(\n" +
                "\t\t\t'connector.type' = 'elasticsearch',\n" +
                "\t\t\t'connector.version' = '6',\n" +
                "\t\t\t'connector.hosts' = 'http://10.101.232.31:9200',\n" +
                "\t\t\t'connector.index' = '8f474082877c41b5ab685355430abfbf_flink_local',\n" +
                "\t\t\t'connector.document-type' = '_doc',\n" +
                "\t\t\t'connector.bulk-flush.max-actions' = '1',\n" +
                "\t\t\t'format.type' = 'json',\n" +
                "\t\t\t'update-mode' = 'append'\n" +
                "\t\t)");

        tableEnv.sqlUpdate("insert\n" +
                "\tinto\n" +
                "\t\tschool(id) select\n" +
                "\t\t\tFWSQRYMC\n" +
                "\t\tfrom\n" +
                "\t\t\twork1");

        // start
        tableEnv.execute(jobName);
    }
}
