//package com.yzhou.connector.http;
//
//import org.apache.flink.api.common.serialization.SerializationSchema;
//import org.apache.flink.table.api.TableSchema;
//import org.apache.flink.table.connector.ChangelogMode;
//import org.apache.flink.table.connector.format.EncodingFormat;
//import org.apache.flink.table.connector.sink.DynamicTableSink;
//import org.apache.flink.table.connector.sink.SinkFunctionProvider;
//import org.apache.flink.table.data.RowData;
//
//import java.util.Objects;
//
//public class HttpDynamicTableSink implements DynamicTableSink {
//
//    private HttpSourceInfo httpSourceInfo;
//    private EncodingFormat<SerializationSchema<RowData>> encodingFormat;
//    private DataType producedDataType;
//    private TableSchema tableSchema;
//
//    public HttpDynamicTableSink() {
//
//    }
//
//    public HttpDynamicTableSink(HttpSourceInfo httpSourceInfo,
//                                EncodingFormat<SerializationSchema<RowData>> encodingFormat,
//                                DataType producedDataType,
//                                TableSchema tableSchema) {
//        this.httpSourceInfo = httpSourceInfo;
//        this.encodingFormat = encodingFormat;
//        this.producedDataType = producedDataType;
//        this.tableSchema = tableSchema;
//    }
//
//    @Override
//    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
//        return changelogMode;
//    }
//
//    // 重点关注地方，主要通过这个方法去构造输出数据源的实现类
//    @Override
//    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
//        final SerializationSchema<RowData> deserializer = encodingFormat.createRuntimeEncoder(
//                context,
//                producedDataType);
//        HttpSinkFunction httpSinkFunction =
//                HttpSinkFunction.builder().setUrl(httpSourceInfo.getUrl())
//                        .setBody(httpSourceInfo.getBody()).setDeserializer(deserializer)
//                        .setType(httpSourceInfo.getType()).setFields(tableSchema.getFieldNames())
//                        .setDataTypes(tableSchema.getFieldDataTypes()).build();
//        return SinkFunctionProvider.of(httpSinkFunction);
//    }
//
//    @Override
//    public DynamicTableSink copy() {
//        return new HttpDynamicTableSink(this.httpSourceInfo, this.encodingFormat,
//                this.producedDataType, this.tableSchema);
//    }
//
//    @Override
//    public String asSummaryString() {
//        return "HTTP Table Sink";
//    }
//
//    public boolean equals(Object o) {
//        if (this == o) {
//            return true;
//        } else if (!(o instanceof HttpDynamicTableSink)) {
//            return false;
//        } else {
//            HttpDynamicTableSink that = (HttpDynamicTableSink) o;
//            return Objects.equals(this.httpSourceInfo, that.httpSourceInfo)
//                    && Objects.equals(this.encodingFormat, that.encodingFormat)
//                    && Objects.equals(this.producedDataType, that.producedDataType);
//        }
//    }
//
//    public int hashCode() {
//        return Objects.hash(new Object[]{this.httpSourceInfo, this.encodingFormat,
//                this.producedDataType, this.tableSchema});
//    }
//}
