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

        // source file
        tableEnv.sqlUpdate("create table work1(\n" +
                "  SJSWJG_DM STRING,\n" +
                "  SLRYS STRING,\n" +
                "  SWJG_DM STRING,\n" +
                "  SLYWS STRING\n" +
                ") with (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'work1',\n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'connector.properties.0.key' = 'zookeeper.connect',\n" +
                "  'connector.properties.0.value' = '10.101.236.2:2181',\n" +
                "  'connector.properties.1.key' = 'bootstrap.servers',\n" +
                "  'connector.properties.1.value' = '10.101.236.2:6667',\n" +
                "  'connector.properties.2.key' = 'group.id',\n" +
                "  'connector.properties.2.value' = '02020080411124458000000101001100',\n" +
                "  'format.type' = 'json',\n" +
                "  'format.derive-schema' = 'true'\n" +
                ")");

        // destination file
        tableEnv.sqlUpdate("create table DM_GY_SWJG_LJS(SWJG_DM STRING, SWJGMC STRING, SWJGJC STRING) with (\n" +
                "  'connector.type' = 'jdbc',\n" +
                "  'connector.url' = 'jdbc:mysql://10.101.236.3:3306/binlog_test?useUnicode=true&characterEncoding=UTF-8',\n" +
                "  'connector.table' = 'DM_GY_SWJG_LJS',\n" +
                "  'connector.username' = 'remote',\n" +
                "  'connector.password' = 'C1stc.0e',\n" +
                "  'connector.write.flush.max-rows' = '1'\n" +
                ")");

        // doing
        tableEnv.sqlUpdate("insert into\n" +
                "  DM_GY_SWJG_LJS(SWJG_DM, SWJGMC, SWJGJC)\n" +
                "select\n" +
                "  SWJG_DM,\n" +
                "  SJSWJG_DM,\n" +
                "  SLYWS\n" +
                "from\n" +
                "  (\n" +
                "    SELECT\n" +
                "      UPPER(SJSWJG_DM) AS SJSWJG_DM,\n" +
                "      SLRYS,\n" +
                "      SWJG_DM,\n" +
                "      SLYWS\n" +
                "    FROM\n" +
                "      work1\n" +
                "  )");

        // start
        tableEnv.execute("wuhan_test");
    }
}
