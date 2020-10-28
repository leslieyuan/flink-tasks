package com.ly.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

public class MysqlDynamicSource implements ScanTableSource {
    private String host;
    private Integer port;
    private String database;
    private String table;
    private String user;
    private String password;
    protected DataType producedDataType;
    protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    public MysqlDynamicSource(
            String hostname,
            int port,
            String database,
            String user,
            String password,
            String tableName,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DataType producedDataType) {
        this.host = Preconditions.checkNotNull(hostname, "Mysql host name can not be null.");
        this.port = Preconditions.checkNotNull(port, "Mysql port name can not be null.");
        this.database = database;
        this.user = user;
        this.password = password;
        this.table = tableName;
        this.decodingFormat = Preconditions.checkNotNull(decodingFormat, "Decoding format must not be null.");
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
//        return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                scanContext,
                producedDataType);

        final SourceFunction<RowData> sourceFunction = new MysqlSourceFunction(
                host,
                port,
                database,
                user,
                password,
                deserializer);

        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
