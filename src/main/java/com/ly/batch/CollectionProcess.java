//package com.ly.batch;
//
//import org.apache.flink.api.java.CollectionEnvironment;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.java.BatchTableEnvironment;
//import org.apache.flink.table.sources.TableSource;
//import org.apache.flink.types.Row;
//
///**
// * @author yuanlong
// * @version 1.0
// * @description
// * @date 2020/6/8 16:19
// */
//
//public class CollectionProcess {
//    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);
//
//        String schema =
//                "{\"type\":\"record\",\"name\":\"root\",\"fields\":[{\"name\":\"parent_process_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"process_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"dst_address\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"technique_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"tgt_path\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"tgt_pid\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"windows_event_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"event_rule_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"dev_address\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"sa_da\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"tran_protocol\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"src_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"domain_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"operation_object\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"protocol\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"event_type\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"vendor\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"src_address\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"source_user\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"image\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"parent_command_line\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"product\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"sa_sp_ap_da_dp\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"rule_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"receive_time\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"collector_source\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"source_process_id\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"src_ad\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"data_source\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"rule_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"src_port\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"event_content\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"file_hash\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"source_image\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"dst_port\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"event_level\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"event_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"parent_image\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"current_directory\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"technique_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"user_account\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"command_line\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"host_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"occur_time\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"row_time\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null}]}";
//
//        ParquestTableSource parquetTableSource = ParquetTableSource
//                .builder()
//                .path("/Users/sujun/Downloads/edr/EDR")
//                .forParquetSchema(new
//                        AvroSchemaConverter().convert(org.apache.avro.Schema.parse(schema,
//                        true)))
//                .build();
//
//
//        Table source = fbTableEnv.fromTableSource(parquetTableSource);
//        fbTableEnv.createTemporaryView("source", source);
//
//        Table table = fbTableEnv.sqlQuery("select event_name from source
//                where event_name = '没有这个值'");
//
//                fbTableEnv.toDataSet(table, Row.class).print();
//
//        fbTableEnv.execute("start");
//    }