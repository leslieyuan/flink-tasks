package com.ly.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

public class MysqlSourceFunction extends RichSourceFunction<RowData> {
    private final String host;
    private final Integer port;
    private final String database;
    private final String user;
    private final String password;
    private volatile boolean running = true;
    private final DeserializationSchema<RowData> deserializationSchema;

    public MysqlSourceFunction(
            String host,
            int port,
            String database,
            String user,
            String password,
            DeserializationSchema<RowData> deserializationSchema) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.password = password;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        while (running) {
            // TODO: continue read bytes from MYSQL table
            byte[] bytes = null;
            sourceContext.collect(deserializationSchema.deserialize(bytes));
        }
    }

    @Override
    public void cancel() {
        running = false;
        try {
            // TODO: stop read
        } catch (Throwable t) {

        }
    }
}
