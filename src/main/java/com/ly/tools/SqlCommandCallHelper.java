package com.ly.tools;

import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class SqlCommandCallHelper {
    private static StreamTableEnvironment environment = null;
    public static void callCommand(SqlCommandParser.SqlCommandCall cmdCall, StreamTableEnvironment env) {
        environment = env;
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

    private static void callSet(SqlCommandParser.SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        environment.getConfig().getConfiguration().setString(key, value);
    }

    private static void callCreateTable(SqlCommandParser.SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            environment.sqlUpdate(ddl);
        } catch (SqlParserException e) {
            String errMsg = String.format("SQL parse failed:\n %s \n %s", ddl, e.getMessage());
            throw new RuntimeException(errMsg);
        }
    }

    private static void callInsertInto(SqlCommandParser.SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        try {
            environment.sqlUpdate(dml);
        } catch (SqlParserException e) {
            String errMsg = String.format("SQL parse failed:\n %s \n %s", dml, e.getMessage());
            throw new RuntimeException(errMsg);
        }
    }
}
