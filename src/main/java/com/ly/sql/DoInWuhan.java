package com.ly.sql;

import com.cestc.sqlsubmit.log4j.Logs;
import com.ly.SqlCommandParser;
import com.ly.tools.ReadFile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class DoInWuhan {
    private static final Logger LOG = LoggerFactory.getLogger(DoInWuhan.class);
    private static final String sql = ReadFile.readFile2String("/wuhan_test.sql");
    private StreamTableEnvironment tEnv;
    private String jobName;

    public static void main(String[] args) throws Exception {
        DoInWuhan task = new DoInWuhan();
        task.jobName = args.length == 0 ? "no name" : args[0];
        Logs.init(task.jobName);
        task.run();
    }

    private void run() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        this.tEnv = StreamTableEnvironment.create(env, settings);

        try {
            List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parseSql(sql);
            for (SqlCommandParser.SqlCommandCall call : calls) {
                callCommand(call);
            }
            tEnv.execute(jobName);
        } catch (FutureUtils.RetryException e) {
            // do nothing
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------

    private void callCommand(SqlCommandParser.SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case SET:
                callSet(cmdCall);
                break;
            case CREATE_TABLE:
                callCreateTable(cmdCall);
                break;
            case INSERT_INTO:
                callInsertInto(cmdCall);
                break;
            default:
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }

    private void callSet(SqlCommandParser.SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        tEnv.getConfig().getConfiguration().setString(key, value);
    }

    private void callCreateTable(SqlCommandParser.SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            tEnv.sqlUpdate(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void callInsertInto(SqlCommandParser.SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        try {
            tEnv.sqlUpdate(dml);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
    }

}
