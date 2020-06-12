package com.ly.plugin;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;

/**
 * @author yuanlong
 * @version 1.0
 * @description
 * @date 2020/6/10 18:11
 */

public class RedisTableSink implements AppendStreamTableSink {
    @Override
    public DataStreamSink<?> consumeDataStream(DataStream dataStream) {
        // todo: return the result data stream
        return null;
    }

    @Deprecated
    public void emitDataStream(DataStream dataStream) {}

    @Override
    public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
        return null;
    }
}