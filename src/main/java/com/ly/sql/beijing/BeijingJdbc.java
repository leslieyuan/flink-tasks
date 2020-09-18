package com.ly.sql.beijing;

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

public class BeijingJdbc {
    private static final Logger LOG = LoggerFactory.getLogger(BeijingJdbc.class);
    private static final String sql = ReadFile.readFile2String("/demo_beijing.sql");
    private StreamTableEnvironment tEnv;
    private String jobName;

    public static void main(String[] args) throws Exception {
        BeijingJdbc task = new BeijingJdbc();
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
