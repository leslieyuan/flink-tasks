package com.ly.sql.wuhan;

import com.cestc.sqlsubmit.log4j.Logs;
import com.ly.tools.ReadFile;
import com.ly.tools.SqlCommandParser;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.ly.tools.SqlCommandCallHelper.callCommand;

/**
 * 实时采集日志处理任务
 * 从kafka写到es
 */
public class CjLogSync {
    private static final Logger LOG = LoggerFactory.getLogger(CjLogSync.class);
    private static final String sql = ReadFile.readFile2String("/log_cj.sql");
    private StreamTableEnvironment tEnv;
    private String jobName;

    public static void main(String[] args) throws Exception {
        CjLogSync task = new CjLogSync();
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
                callCommand(call, tEnv);
            }
            tEnv.execute(jobName);
        } catch (FutureUtils.RetryException e) {
            // do nothing
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
}
