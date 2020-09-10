package com.ly.sql.beijing;

import com.ly.sql.demo.ParseNestedJsonWin;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoInBeijing {
    private static final Logger log = LoggerFactory.getLogger(ParseNestedJsonWin.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        String jobName = args[0];

        tableEnv.sqlUpdate("create\n" +
                "\ttable\n" +
                "\t\twork1(\n" +
                "\t\t\tFWSQRYMC VARCHAR\n" +
                "\t\t) with(\n" +
                "\t\t\t'connector.type' = 'kafka',\n" +
                "\t\t\t'connector.version' = 'universal',\n" +
                "\t\t\t'connector.topic' = 'work2',\n" +
                "\t\t\t'connector.startup-mode' = 'latest-offset',\n" +
                "\t\t\t'connector.properties.0.key' = 'zookeeper.connect',\n" +
                "\t\t\t'connector.properties.0.value' = '172.17.78.247:2181',\n" +
                "\t\t\t'connector.properties.1.key' = 'bootstrap.servers',\n" +
                "\t\t\t'connector.properties.1.value' = '172.17.78.247:6667',\n" +
                "\t\t\t'connector.properties.2.key' = 'group.id',\n" +
                "\t\t\t'connector.properties.2.value' = '02020062216021101400000101001103',\n" +
                "\t\t\t'format.type' = 'json',\n" +
                "\t\t\t'format.derive-schema' = 'true'\n" +
                "\t\t)");

        tableEnv.sqlUpdate("create\n" +
                "\ttable\n" +
                "\t\tschool(\n" +
                "\t\t\tid varchar\n" +
                "\t\t) with(\n" +
                "\t\t\t'connector.type' = 'jdbc',\n" +
                "\t\t\t'connector.url' = 'jdbc:mysql://172.17.78.250:3306/binlog_test',\n" +
                "\t\t\t'connector.table' = 'school',\n" +
                "\t\t\t'connector.username' = 'remote',\n" +
                "\t\t\t'connector.password' = 'Cestc.02',\n" +
                "\t\t\t'connector.write.flush.max-rows' = '1'\n" +
                "\t\t)");

        tableEnv.sqlUpdate("insert\n" +
                "\tinto\n" +
                "\t\tschool(id) select\n" +
                "\t\t\tFWSQRYMC\n" +
                "\t\tfrom\n" +
                "\t\t\twork1");

        tableEnv.execute(jobName);
    }
}
