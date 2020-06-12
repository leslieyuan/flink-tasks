package com.ly.play;

import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author yuanlong
 * @version 1.0
 * @description
 * @date 2020/6/11 16:11
 */

public class TestOracle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        tableEnv.sqlUpdate("create table s_kafka\n" +
                "(\n" +
                "  v_operation varchar,\n" +
                "  v_timestamp bigint,\n" +
                "  v_table_name varchar,\n" +
                "  v_database varchar,\n" +
                "  TID varchar,\n" +
                "  PROVINCE varchar,\n" +
                "  CITY varchar,\n" +
                "  DISTRICT varchar,\n" +
                "  TVALUE decimal(38,18)\n" +
                ")\n" +
                "WITH(\n" +
                "'connector.type' = 'kafka',\n" +
                "'connector.properties.group.id' = 'wuqingGroupTest',\n" +
                "'connector.topic' = 'test4_wuqing',\n" +
                "'connector.startup-mode' = 'earliest-offset',\n" +
                "'connector.properties.zookeeper.connect' = 'hdp1.ambari:2181',\n" +
                "'connector.properties.bootstrap.servers' = '10.101.236.2:6667',\n" +
                "'format.type' = 'json', \n" +
                "'connector.version' = 'universal'\n" +
                ")");

        tableEnv.sqlUpdate("create table o_flink_sink\n" +
                "(\n" +
                "  PROVINCE varchar,\n" +
                "  CITY varchar,\n" +
                "  DISTRICT varchar,\n" +
                "  CSUM decimal(38,18)\n" +
                ")\n" +
                "with (\n" +
                "  'connector.type' = 'jdbc',\n" +
                "  'connector.url' = 'jdbc:oracle:thin:@10.101.232.115:1521:orcl',\n" +
                "  'connector.table' = 'O_FLINK_SINK',\n" +
                "  'connector.username' = 'u_test',\n" +
                "  'connector.password' = 'qazWSX123',\n" +
                "  'connector.write.flush.max-rows' = '1'\n" +
                ")");

        tableEnv.sqlUpdate("INSERT INTO o_flink_sink\n" +
                "SELECT PROVINCE,CITY,DISTRICT,SUM(TVALUE) CSUM\n" +
                "FROM s_kafka\n" +
                "GROUP BY PROVINCE,CITY,DISTRICT");

        tableEnv.execute("oracle_test");
    }
}