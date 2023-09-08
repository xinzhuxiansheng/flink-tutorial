package com.yzhou.cdc.mysql;

import com.yzhou.cdc.mysql.sink.CreateMysqlLSinkTable;
import com.yzhou.cdc.mysql.source.MysqlCdcSource;
import com.yzhou.cdc.mysql.util.MysqlUtil;
import com.yzhou.cdc.mysql.util.ParameterUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.databases.mysql.catalog.MySqlCatalog;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DefaultCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * MySQL 2 MySQL 整库同步
 */
public class WholeMySQLDatabase2MySQLExample {
    private static final Logger LOG = LoggerFactory.getLogger(WholeMySQLDatabase2MySQLExample.class);

    public static void main(String[] args) throws Exception {

        String ckPathAndJobId = "";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // set sql job name
        tEnv.getConfig().getConfiguration().setString("pipeline.name", ckPathAndJobId);

        checkpoint(env,ckPathAndJobId,true,true);

        restartTask(env);

        handle(args,env,tEnv);

    }

    public static void checkpoint(
            StreamExecutionEnvironment env,
            String ckPathAndJobId,
            Boolean hashMap,
            Boolean localpath) {
        if (hashMap) {
            env.setStateBackend(new HashMapStateBackend());
        } else {
            // 该类型 State Backend 支持 Changelog 增量检查点
            env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        }
        if (localpath) {
            env.enableCheckpointing(5 * 60 * 1000);
            // 注意这里默认把状态存储在内存中，如内存打满将导致 checkpoint 失败
            // 测试任务如数据量较大请指定文件存储
            // env.getCheckpointConfig()
            //    .setCheckpointStorage("file:///user/flink/" + ckPathAndJobId);
        } else {
            env.getCheckpointConfig()
                    .setCheckpointStorage("hdfs://hadoop1:8020/flink/" + ckPathAndJobId);
            // Hadoop HA 写法：
            // hdfs://nameService_id/path/file
            env.enableCheckpointing(60 * 1000);
        }
        // Changelog 是一项旨在减少检查点时间的功能，因此可以减少一次模式下的端到端延迟。
        // 在 EmbeddedRocksDBStateBackend 中受到支持
        env.enableChangelogStateBackend(false); // 启用 Changelog 可能会对应用程序的性能产生负面影响。
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(3 * 60 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    public static void restartTask(StreamExecutionEnvironment env) {
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(10)));
    }

    public static void handle(String[] args, StreamExecutionEnvironment env, StreamTableEnvironment tEnv)
            throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        String databaseName = ParameterUtil.databaseName(params);
        String tableList = ParameterUtil.tableList(params);

        String connectorWithBody = CreateMysqlLSinkTable.connectorWithBody(params);

        // 注册同步的库对应的 Catalog 这里是 mysql catalog

        MySqlCatalog mySqlCatalog = MysqlUtil.useMysqlCatalog(params);

        List<String> tables;

        // 如果整库同步，则从 Catalog 里取所有表，否则从指定表中取表名
        try {
            if (".*".equals(tableList)) {
                tables = mySqlCatalog.listTables(databaseName);
            } else {
                if (tableList.contains(",")) {
                    String[] tableArray = tableList.split(",");
                    tables =
                            Arrays.stream(tableArray)
                                    .map(table -> table.split("\\.")[1])
                                    .collect(Collectors.toList());
                } else {
                    tables = Collections.singletonList(tableList);
                }
            }
        } catch (DatabaseNotExistException e) {
            LOG.error("{} 库不存在", databaseName, e);
            throw e;
        }
        // 创建表名和对应 RowTypeInfo 映射的 Map
        Map<String, RowTypeInfo> tableTypeInformationMap = Maps.newConcurrentMap();
        Map<String, DataType[]> tableDataTypesMap = Maps.newConcurrentMap();
        Map<String, RowType> tableRowTypeMap = Maps.newConcurrentMap();
        for (String table : tables) {
            // 获取  Catalog 中注册的表
            ObjectPath objectPath = new ObjectPath(databaseName, table);
            DefaultCatalogTable catalogBaseTable;
            try {
                catalogBaseTable = (DefaultCatalogTable) mySqlCatalog.getTable(objectPath);

            } catch (TableNotExistException e) {
                LOG.error("{} 表不存在", table, e);
                throw e;
            }
            // 获取表的 Schema
            assert catalogBaseTable != null;
            Schema schema = catalogBaseTable.getUnresolvedSchema();
            // 获取表中字段名列表
            String[] fieldNames = new String[schema.getColumns().size()];
            // 获取DataType
            DataType[] fieldDataTypes = new DataType[schema.getColumns().size()];
            LogicalType[] logicalTypes = new LogicalType[schema.getColumns().size()];

            // 获取表字段类型
            TypeInformation<?>[] fieldTypes = new TypeInformation[schema.getColumns().size()];
            // 获取表的主键
            List<String> primaryKeys;
            try {
                primaryKeys = schema.getPrimaryKey().get().getColumnNames(); // 此处不用 orElse
            } catch (NullPointerException e) {
                LOG.error("捕捉表异常: {} 表没有主键！！！ 当前 mysql cdc 尚不支持捕捉没有主键的表！！！", table, e);
                throw e;
            }

            for (int i = 0; i < schema.getColumns().size(); i++) {
                Schema.UnresolvedPhysicalColumn column =
                        (Schema.UnresolvedPhysicalColumn) schema.getColumns().get(i);
                fieldNames[i] = column.getName();
                fieldDataTypes[i] = (DataType) column.getDataType();
                fieldTypes[i] =
                        InternalTypeInfo.of(((DataType) column.getDataType()).getLogicalType());
                logicalTypes[i] = ((DataType) column.getDataType()).getLogicalType();
            }
            RowType rowType = RowType.of(logicalTypes, fieldNames);
            tableRowTypeMap.put(table, rowType);

            // 组装 Flink Sink 表 DDL sql
            StringBuilder stmt = new StringBuilder();
            String sinkTableName =
                    String.format(params.get("sinkPrefix", "sink_%s"), table); // Sink 表前缀
            stmt.append("create table if not exists ").append(sinkTableName).append("(\n");

            for (int i = 0; i < fieldNames.length; i++) {
                String column = fieldNames[i];
                String fieldDataType = fieldDataTypes[i].toString();
                stmt.append("\t`").append(column).append("` ").append(fieldDataType).append(",\n");
            }

            stmt.append(
                    String.format(
                            "PRIMARY KEY (%s) NOT ENFORCED\n)",
                            StringUtils.join(primaryKeys, ",")));
            String formatJdbcSinkWithBody =
                    connectorWithBody.replace("${sinkTableName}", sinkTableName);
            String createSinkTableDdl = stmt + formatJdbcSinkWithBody;
            // 创建 Flink Sink 表
            LOG.info("createSinkTableDdl: \r {}", createSinkTableDdl);
            tEnv.executeSql(createSinkTableDdl);
            tableDataTypesMap.put(table, fieldDataTypes);
            tableTypeInformationMap.put(table, new RowTypeInfo(fieldTypes, fieldNames));

            // 下游 MySQL 建表逻辑
            new CreateMysqlLSinkTable()
                    .createMysqlSinkTable(
                            params, sinkTableName, fieldNames, fieldDataTypes, primaryKeys);
        }

        //  MySQL CDC
        SingleOutputStreamOperator<Tuple2<String, Row>> dataStreamSource =
                new MysqlCdcSource()
                        .singleOutputStreamOperator(params, env, tableRowTypeMap); // 切断任务链
        StatementSet statementSet = tEnv.createStatementSet();
        // DataStream 转 Table，创建临时视图，插入 sink 表
        for (Map.Entry<String, RowTypeInfo> entry : tableTypeInformationMap.entrySet()) {
            String tableName = entry.getKey();
            RowTypeInfo rowTypeInfo = entry.getValue();
            SingleOutputStreamOperator<Row> mapStream =
                    dataStreamSource
                            .filter(data -> data.f0.equals(tableName))
                            .setParallelism(ParameterUtil.setParallelism(params))
                            .map(data -> data.f1, rowTypeInfo)
                            .setParallelism(ParameterUtil.setParallelism(params));
            Table table = tEnv.fromChangelogStream(mapStream);
            String temporaryViewName = String.format("t_%s", tableName);
            tEnv.createTemporaryView(temporaryViewName, table);
            String sinkTableName = String.format("sink_%s", tableName);
            String insertSql =
                    String.format(
                            "insert into %s select * from %s", sinkTableName, temporaryViewName);
            LOG.info("add insertSql for {}, sql: {}", tableName, insertSql);
            statementSet.addInsertSql(insertSql);
        }
        statementSet.execute();
    }

}
