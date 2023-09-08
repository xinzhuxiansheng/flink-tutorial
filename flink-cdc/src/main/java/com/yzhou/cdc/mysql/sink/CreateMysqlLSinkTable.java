package com.yzhou.cdc.mysql.sink;

import com.yzhou.cdc.mysql.util.MysqlUtil;
import com.yzhou.cdc.mysql.util.ParameterUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.types.DataType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class CreateMysqlLSinkTable {
    public void createMysqlSinkTable(
            ParameterTool params,
            String sinkTableName,
            String[] fieldNames,
            DataType[] fieldDataTypes,
            List<String> primaryKeys)
            throws SQLException, ClassNotFoundException {
        String createSql =
                MysqlUtil.createTable(sinkTableName, fieldNames, fieldDataTypes, primaryKeys);
        Connection connection =
                MysqlUtil.getConnection(
                        ParameterUtil.sinkUrl(params),
                        ParameterUtil.sinkUsername(params),
                        ParameterUtil.sinkPassword(params));
        MysqlUtil.executeSql(connection, createSql);
    }

    public static String connectorWithBody(ParameterTool params) {
        String connectorWithBody =
                " with (\n"
                        + " 'connector' = '${sinkType}',\n"
                        + " 'url' = '${sinkUrl}',\n"
                        + " 'username' = '${sinkUsername}',\n"
                        + " 'password' = '${sinkPassword}',\n"
                        + " 'table-name' = '${sinkTableName}'\n"
                        + ")";

        connectorWithBody =
                connectorWithBody
                        .replace("${sinkType}", "jdbc")
                        .replace("${sinkUrl}", ParameterUtil.sinkUrl(params))
                        .replace("${sinkUsername}", ParameterUtil.sinkUsername(params))
                        .replace("${sinkPassword}", ParameterUtil.sinkPassword(params));

        return connectorWithBody;
    }
}
