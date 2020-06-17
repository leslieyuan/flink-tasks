package com.ly.sql;

import com.ly.log4j.CestcJsonLayout;
import com.ly.log4j.Logs;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yuanlong
 * @version 1.0
 * @description
 * @date 2020/6/15 16:25
 */

public class PerformanceTest {
    private static final Logger log = LoggerFactory.getLogger(PerformanceTest.class);

    public static void main(String[] args) {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        final String rwid = args[0];
        CestcJsonLayout.setRwid(rwid);
        CestcJsonLayout.setRwzt("RUNNING");
        Logs.init();

        try {

            tableEnv.sqlUpdate("CREATE TABLE source_info(\n" +
                    "user_id VARCHAR,\n" +
                    "item_id VARCHAR,\n" +
                    "category_id VARCHAR ,\n" +
                    "behavior VARCHAR ,\n" +
                    "ts VARCHAR\n" +
                    ") WITH (\n" +
                    "'connector.type' = 'kafka',\n" +
                    "'connector.version' = 'universal',\n" +
                    "'connector.topic' = 'xingneng2',\n" +
                    "'connector.properties.group.id' = 'performance',\n" +
                    "'connector.startup-mode' = 'earliest-offset',\n" +
                    "'connector.properties.zookeeper.connect' = '10.101.236.2:2181',\n" +
                    "'connector.properties.bootstrap.servers' = '10.101.236.2:6667',\n" +
                    "'update-mode' = 'append',\n" +
                    "'format.type' = 'json',\n" +
                    "'format.derive-schema' = 'true'\n" +
                    "  )");

            tableEnv.sqlUpdate("CREATE TABLE behavior_info(\n" +
                    "user_id VARCHAR ,\n" +
                    "item_id VARCHAR ,\n" +
                    "category_id VARCHAR ,\n" +
                    "behavior VARCHAR\n" +
                    ") WITH (\n" +
                    "'connector.type' = 'kafka',\n" +
                    "'connector.version' = 'universal',\n" +
                    "'connector.topic' = 'xingneng',\n" +
                    "'connector.properties.zookeeper.connect' = '10.101.236.2:2181',\n" +
                    "'connector.properties.bootstrap.servers' = '10.101.236.2:6667',\n" +
                    "'update-mode' = 'append',\n" +
                    "'format.type' = 'json',\n" +
                    "'format.derive-schema' = 'true'\n" +
                    "  )");

            tableEnv.sqlUpdate("INSERT INTO behavior_info SELECT user_id, item_id, category_id, behavior FROM source_info");

            tableEnv.execute("performance_test");
        } catch (Exception e) {
            CestcJsonLayout.setRwzt("FAILED");
            log.error(e.getMessage());
        }
    }
}