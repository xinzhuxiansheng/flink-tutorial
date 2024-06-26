package com.yzhou.connector.http;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * 用于将 CatalogTable 的元数据转换为 DynamicTableSource
 */
public class HttpDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    // 定义所有配置项
    // define all options statically
    public static final ConfigOption<String> URL = ConfigOptions.key("http.url")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> METHOD = ConfigOptions.key("http.method")
            .stringType()
            .defaultValue("get");

    /*
        read.streaming.enabled
        read.streaming.check-interval
        这两个参数控制 HttpSourceFunction#run()读取方式
     */
    public static final ConfigOption<Boolean> READ_AS_STREAMING = ConfigOptions.key("read.streaming.enabled")
            .booleanType()
            .defaultValue(false)// default read as batch
            .withDescription("Whether to read as streaming source, default false");

    public static final ConfigOption<Long> READ_STREAMING_CHECK_INTERVAL = ConfigOptions.key("read.streaming.check-interval")
            .longType()
            .defaultValue(60L)// default 1 minute
            .withDescription("Check interval for streaming read of SECOND, default 1 minute");


    @Override
    public String factoryIdentifier() {
        // 用于匹配： `connector = '...'`
        return "http"; // used for matching to `connector = '...'`
    }

    /**
     * 要求参数
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URL);
        options.add(METHOD);
        // 解码的格式器使用预先定义的配置项
        options.add(FactoryUtil.FORMAT); // use pre-defined option for format
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(READ_AS_STREAMING);
        options.add(READ_STREAMING_CHECK_INTERVAL);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // 使用提供的工具类或实现你自己的逻辑进行校验
        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // 找到合适的解码器
        // discover a suitable decoding format
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);

        // 校验所有的配置项
        // validate all options
        helper.validate();

        // 获取校验完的配置项
        // get the validated options
        final ReadableConfig options = helper.getOptions();
        final String url = options.get(URL);
        final String method = options.get(METHOD);
        final boolean isStreaming = options.get(READ_AS_STREAMING);
        final long interval = options.get(READ_STREAMING_CHECK_INTERVAL);

        // 从 catalog 中抽取要生产的数据类型 (除了需要计算的列)
        // derive the produced data type (excluding computed columns) from the catalog table
        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        // 创建并返回动态表 source
        // create and return dynamic table source
        return new HttpDynamicTableSource(url, method, isStreaming, interval, decodingFormat, producedDataType);
    }

//    @Override
//    public DynamicTableSink createDynamicTableSink(Context context) {
//        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
//        ReadableConfig config = helper.getOptions();
//        // 校验参数
//        helper.validate();
//        // 自定义一些配置校验参数
//        this.validateConfigOptions(config);
//        HttpSourceInfo httpSourceInfo = this.getHttpSource(config);
//        // discover a suitable encoding format
//        final EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
//                SerializationFormatFactory.class,
//                FactoryUtil.FORMAT);
//
//        // derive the produced data type (excluding computed columns) from the catalog table
//        final DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
//        TableSchema tableSchema = context.getCatalogTable().getSchema();
//        return new HttpDynamicTableSink(httpSourceInfo, encodingFormat, producedDataType, tableSchema);
//    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return DynamicTableSourceFactory.super.forwardOptions();
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return null;
    }


    // 参数校验，根据实际情况去实现需要校验哪些参数，比如有些参数有格式校验可以在这里实现，没有可以不实现
//    private void validateConfigOptions(ReadableConfig config) {
//        String url = config.get(URL);
//        Optional<String> urlOp = Optional.of(url);
//        Preconditions.checkState(urlOp.isPresent(), "Cannot handle such http url: " + url);
//        String type = config.get(TYPE);
//        if ("POST".equalsIgnoreCase(type)) {
//            String body = config.get(BODY);
//            Optional<String> bodyOp = Optional.of(body);
//            Preconditions.checkState(bodyOp.isPresent(), "Cannot handle such http post body: " + bodyOp);
//        }
//    }
}
