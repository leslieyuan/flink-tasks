package com.ly.sql.beijing50;

import com.ly.sql.ParseNestedJsonWin;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Kafka {
    private static final Logger log = LoggerFactory.getLogger(ParseNestedJsonWin.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        String jobName = args[0];

        tableEnv.sqlUpdate("CREATE\n" +
                "\tTABLE\n" +
                "\t\tt_nftywll(\n" +
                "\t\t\tv_operation VARCHAR,\n" +
                "\t\t\tv_timestamp VARCHAR,\n" +
                "\t\t\tv_table_name VARCHAR,\n" +
                "\t\t\tv_database VARCHAR,\n" +
                "\t\t\tSJSWJG_DM VARCHAR,\n" +
                "\t\t\tSJSWJGMC VARCHAR,\n" +
                "\t\t\tSWJG_DM VARCHAR,\n" +
                "\t\t\tSWJGMC VARCHAR,\n" +
                "\t\t\tFWQDLX_DM VARCHAR,\n" +
                "\t\t\tYWLSID VARCHAR,\n" +
                "\t\t\tYWSLRYMC VARCHAR,\n" +
                "\t\t\tFWSQRYMC VARCHAR,\n" +
                "\t\t\tYWLX_DM VARCHAR,\n" +
                "\t\t\tKSPDSJ BIGINT,\n" +
                "\t\t\tKSBLYWSJ BIGINT,\n" +
                "\t\t\tJSBLYWSJ BIGINT\n" +
                "\t\t) WITH(\n" +
                "\t\t\t'connector.type' = 'kafka',\n" +
                "\t\t\t'connector.version' = 'universal',\n" +
                "\t\t\t'connector.topic' = 'yuanlongtest',\n" +
                "\t\t\t'connector.startup-mode' = 'earliest-offset',\n" +
                "\t\t\t'connector.properties.0.key' = 'zookeeper.connect',\n" +
                "\t\t\t'connector.properties.0.value' = 'pk22.bigdata.cestc:2181,pk23.bigdata.cestc:2181,pk24.bigdata.cestc:2181',\n" +
                "\t\t\t'connector.properties.1.key' = 'bootstrap.servers',\n" +
                "\t\t\t'connector.properties.1.value' = '172.17.77.225:6667,172.17.77.225:6667,172.17.77.225:6667',\n" +
                "\t\t\t'connector.properties.2.key' = 'group.id',\n" +
                "\t\t\t'connector.properties.2.value' = 'test_2_kafka',\n" +
                "\t\t\t'update-mode' = 'append',\n" +
                "\t\t\t'format.type' = 'json',\n" +
                "\t\t\t'format.derive-schema' = 'true'\n" +
                "\t\t)");

        tableEnv.sqlUpdate("CREATE\n" +
                "\tTABLE\n" +
                "\t\tT_YY_ZHDP_SWDJZTJK_BSTHXZL_LJS_yuanlong(\n" +
                "\t\t\tSJSWJG_DM VARCHAR,\n" +
                "\t\t\tSJSWJGMC VARCHAR,\n" +
                "\t\t\tPJDDHS DOUBLE,\n" +
                "\t\t\tPJBLHS DOUBLE\n" +
                "\t\t) WITH(\n" +
                "\t\t\t'connector.type' = 'kafka',\n" +
                "\t\t\t'connector.version' = 'universal',\n" +
                "\t\t\t'connector.topic' = 'yuanlongtest2',\n" +
                "\t\t\t'connector.properties.0.key' = 'zookeeper.connect',\n" +
                "\t\t\t'connector.properties.0.value' = 'pk22.bigdata.cestc:2181,pk23.bigdata.cestc:2181,pk24.bigdata.cestc:2181',\n" +
                "\t\t\t'connector.properties.1.key' = 'bootstrap.servers',\n" +
                "\t\t\t'connector.properties.1.value' = '172.17.77.225:6667,172.17.77.225:6667,172.17.77.225:6667',\n" +
                "\t\t\t'connector.properties.2.key' = 'group.id',\n" +
                "\t\t\t'connector.properties.2.value' = 'test_2_mysql',\n" +
                "\t\t\t'update-mode' = 'append',\n" +
                "\t\t\t'format.type' = 'json',\n" +
                "\t\t\t'format.derive-schema' = 'true'\n" +
                "\t\t)");

        tableEnv.sqlUpdate("INSERT\n" +
                "\tINTO\n" +
                "\t\tT_YY_ZHDP_SWDJZTJK_BSTHXZL_LJS_yuanlong SELECT\n" +
                "\t\t\tSJSWJG_DM,\n" +
                "\t\t\tSJSWJGMC,\n" +
                "\t\t\tKSBLYWSJ,\n" +
                "\t\t\tJSBLYWSJ\n" +
                "\t\tFROM\n" +
                "\t\t\tt_nftywll");


        tableEnv.execute(jobName);
    }
}
