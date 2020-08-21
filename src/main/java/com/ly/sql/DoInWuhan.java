package com.ly.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DoInWuhan {
    public static void main(String[] args) throws Exception {
        // 环境变量
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String jobName = args[0];

        // source file
        bsTableEnv.executeSql("CREATE TABLE t_nftywll(\n" +
                "  v_operation VARCHAR,\n" +
                "  v_timestamp VARCHAR,\n" +
                "  v_table_name VARCHAR,\n" +
                "  v_database VARCHAR,\n" +
                "  SJSWJG_DM VARCHAR,\n" +
                "  SJSWJGMC VARCHAR,\n" +
                "  SWJG_DM VARCHAR,\n" +
                "  SWJGMC VARCHAR,\n" +
                "  FWQDLX_DM VARCHAR,\n" +
                "  YWLSID VARCHAR,\n" +
                "  YWSLRYMC VARCHAR,\n" +
                "  FWSQRYMC VARCHAR,\n" +
                "  YWLX_DM VARCHAR,\n" +
                "  KSPDSJ BIGINT,\n" +
                "  KSBLYWSJ BIGINT,\n" +
                "  JSBLYWSJ BIGINT\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'yuanlongtest',\n" +
                " 'properties.bootstrap.servers' = 'hdp1.ambari:6667',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'earliest-offset',\n" +
                " 'sink.partitioner' = 'round-robin'\n" +
                ")", jobName);

        bsTableEnv.executeSql("CREATE TABLE T_YY_ZHDP_SWDJZTJK_BSTHXZL_LJS_yuanlong(\n" +
                "  SJSWJG_DM VARCHAR,\n" +
                "  SJSWJGMC VARCHAR,\n" +
                "  SWJG_DM VARCHAR,\n" +
                "  SWJGMC VARCHAR,\n" +
                "  KFCKS DOUBLE,\n" +
                "  KFZDS DOUBLE,\n" +
                "  SLYWS DOUBLE,\n" +
                "  SLRYS DOUBLE,\n" +
                "  PJDDHS DOUBLE,\n" +
                "  PJBLHS DOUBLE,\n" +
                "  PRIMARY KEY (SJSWJG_DM, SJSWJGMC, SWJG_DM, SWJGMC) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://10.101.236.3:3306/binlog_test?useUnicode=true&characterEncoding=UTF-8',\n" +
                "  'table-name' = 'T_YY_ZHDP_SWDJZTJK_BSTHXZL_LJS_yuanlong',\n" +
                "  'username' = 'remote',\n" +
                "  'password' = 'C1stc.0e',\n" +
                "  'lookup.max-retries' = '3',\n" +
                "  'sink.buffer-flush.max-rows' = '500',\n" +
                "  'sink.buffer-flush.interval' = '2s',\n" +
                "  'sink.max-retries' = '3'\n" +
                ")", jobName);

        bsTableEnv.executeSql("INSERT INTO\n" +
                "  T_YY_ZHDP_SWDJZTJK_BSTHXZL_LJS_yuanlong\n" +
                "SELECT\n" +
                "  SJSWJG_DM,\n" +
                "  SJSWJGMC,\n" +
                "  SWJG_DM,\n" +
                "  SWJGMC,\n" +
                "  SUM(\n" +
                "    CASE\n" +
                "      when FWQDLX_DM = '01' then 1\n" +
                "      else 0\n" +
                "    end\n" +
                "  ) KFCKS,\n" +
                "  SUM(\n" +
                "    CASE\n" +
                "      when FWQDLX_DM = '02' then 1\n" +
                "      else 0\n" +
                "    end\n" +
                "  ) KFZDS,\n" +
                "  COUNT(1) SLYWS,\n" +
                "  COUNT(DISTINCT FWSQRYMC) SLRYS,\n" +
                "  ROUND(SUM((KSBLYWSJ - KSPDSJ) * 24 * 60), 2) PJDDHS,\n" +
                "  ROUND(SUM((JSBLYWSJ - KSBLYWSJ) * 24 * 60), 2) PJBLHS\n" +
                "FROM\n" +
                "  t_nftywll\n" +
                "GROUP BY\n" +
                "  SJSWJG_DM,\n" +
                "  SJSWJGMC,\n" +
                "  SWJG_DM,\n" +
                "  SWJGMC", jobName);

        // start
    }
}
