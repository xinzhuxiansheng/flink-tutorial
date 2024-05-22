//package com.yzhou.connector.http;
//
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.table.data.RowData;
//import org.apache.flink.table.types.logical.DecimalType;
//import org.apache.flink.table.types.logical.TimestampType;
//
//import java.io.IOException;
//import java.sql.Date;
//import java.time.LocalDate;
//import java.util.HashMap;
//
//public class HttpSinkFunction extends RichSinkFunction<RowData> {
//
//    private String url;
//    private String body;
//    private String type;
//    private SerializationSchema<RowData> serializer;
//    private String[] fields;
//    private DataType[] dataTypes;
//    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
//    private final SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//    private static final long serialVersionUID = 1L;
//
//    public HttpSinkFunction() {
//
//    }
//
//    public HttpSinkFunction(String url, String body, String type,
//                            SerializationSchema<RowData> serializer,
//                            String[] fields,
//                            DataType[] dataTypes) {
//        this.url = url;
//        this.body = body;
//        this.type = type;
//        this.serializer = serializer;
//        this.fields = fields;
//        this.dataTypes = dataTypes;
//    }
//
//    // 重点关注，这个invoke方法实现对数据的写出，参数RowData value就是需要输出的数据，这个对象里面具体有多少数据是不确定的，因为默认是流式输出，如果需要考虑性能问题（并且对于实时性没有太高要求），可以自定义实现批量输出，先把这个里面的数据缓存起来，然后当一定时间，或者数据量达到一定阈值的时候再去调研接口输出数据。
//    @Override
//    public void invoke(RowData value, Context context) throws Exception {
//        Object[] objValue =  transform(value);
//        HashMap<String, Object> map = new HashMap<>();
//        for (int i = 0; i < fields.length; i++) {
//            map.put(fields[i], objValue[i]);
//        }
//        String body = PluginUtil.objectToString(map);
//        DtHttpClient.post(url, body);
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        serializer.open(() -> getRuntimeContext().getMetricGroup());
//    }
//
//    @Override
//    public void close() throws IOException {
//
//    }
//
//    public static HttpSinkFunction.Builder builder() {
//        return new HttpSinkFunction.Builder();
//    }
//
//    public static class Builder {
//        private String url;
//        private String body;
//        private String type;
//        private SerializationSchema<RowData> serializer;
//        private String[] fields;
//        private DataType[] dataTypes;
//
//        public Builder () {
//
//        }
//
//        public Builder setUrl(String url) {
//            this.url = url;
//            return this;
//        }
//
//        public Builder setFields(String[] fields) {
//            this.fields = fields;
//            return this;
//        }
//
//        public Builder setBody(String body) {
//            this.body = body;
//            return this;
//        }
//
//        public Builder setType(String type) {
//            this.type = type;
//            return this;
//        }
//
//        public Builder setDataTypes(DataType[] dataTypes) {
//            this.dataTypes = dataTypes;
//            return this;
//        }
//
//        public Builder setDeserializer(SerializationSchema<RowData> serializer) {
//            this.serializer = serializer;
//            return this;
//        }
//
//        public HttpSinkFunction build() {
//            if (StringUtils.isBlank(url) || StringUtils.isBlank(body) || StringUtils.isBlank(type)) {
//                throw new IllegalArgumentException("params has null");
//            }
//            return new HttpSinkFunction(this.url, this.body, this.type, this.serializer,
//                    this.fields, this.dataTypes);
//        }
//
//    }
//
//
//    // 这个方法是用来把RowData对象转换为HTTP接口能够识别的JSON对象的，因为默认HTTP接口不能识别这种复杂对象并且转换为我们常用的JSON对象，所以需要我们自己去解析。当然直接把这个对象丢给HTTP也是可以的，那么就需要在接收方去解析RowData对象，但是默认来说，肯定解析为更通用的类型最合适
//    public Object[] transform(RowData record) {
//        Object[] values = new Object[dataTypes.length];
//        int idx = 0;
//        int var6 = dataTypes.length;
//
//        for (int i = 0; i < var6; ++i) {
//            DataType dataType = dataTypes[i];
//            values[idx] = this.typeConvertion(dataType.getLogicalType(), record, idx);
//            ++idx;
//        }
//
//        return values;
//    }
//
//    private Object typeConvertion(LogicalType type, RowData record, int pos) {
//        if (record.isNullAt(pos)) {
//            return null;
//        } else {
//            switch (type.getTypeRoot()) {
//                case BOOLEAN:
//                    return record.getBoolean(pos) ? 1L : 0L;
//                case TINYINT:
//                    return record.getByte(pos);
//                case SMALLINT:
//                    return record.getShort(pos);
//                case INTEGER:
//                    return record.getInt(pos);
//                case BIGINT:
//                    return record.getLong(pos);
//                case FLOAT:
//                    return record.getFloat(pos);
//                case DOUBLE:
//                    return record.getDouble(pos);
//                case CHAR:
//                case VARCHAR:
//                    return record.getString(pos).toString();
//                case DATE:
//                    return this.dateFormatter.format(Date.valueOf(LocalDate.ofEpochDay(record.getInt(pos))));
//                case TIMESTAMP_WITHOUT_TIME_ZONE:
//                    int timestampPrecision = ((TimestampType) type).getPrecision();
//                    return this.dateTimeFormatter.format(new Date(record.getTimestamp(pos, timestampPrecision)
//                            .toTimestamp().getTime()));
//                case DECIMAL:
//                    int decimalPrecision = ((DecimalType) type).getPrecision();
//                    int decimalScale = ((DecimalType) type).getScale();
//                    return record.getDecimal(pos, decimalPrecision, decimalScale).toBigDecimal();
//                default:
//                    throw new UnsupportedOperationException("Unsupported type:" + type);
//            }
//        }
//    }
//}
