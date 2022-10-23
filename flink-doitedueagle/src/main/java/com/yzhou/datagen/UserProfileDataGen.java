package com.yzhou.datagen;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author yzhou
 * @date 2022/10/23
 */
public class UserProfileDataGen {
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        //conf.set("hbase.zookeeper.quorum", "aidb:2181");

        //conf.set("hbase.zookeeper.quorum", "k8s-node02:2181");
        //conf.set("hbase.master", "k8s-node02:16000");

        conf.set("hbase.zookeeper.quorum", "aibu01:2181,aibu02:2181,aibu03:2181,aidb:2181");
        //conf.set("hbase.zookeeper.property.clientPort","2181");
        //conf.set("hbase.master", "aidb:16000");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        //conf.setInt("hbase.regionserver.port", 16020);
        //conf.set("hbase.master", "k8s-node02:16010");
        //conf.set("zookeeper.znode.parent","/hbase");
        conf.setInt("hbase.rpc.timeout", 10000);
        conf.setInt("hbase.client.operation.timeout", 10000);
        conf.setInt("hbase.client.scanner.timeout.period", 10000);


        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("zenniu_profile"));

        ArrayList<Put> puts = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {

            // 生成一个用户的画像标签数据
            String deviceId = StringUtils.leftPad(i + "", 6, "0");
            Put put = new Put(Bytes.toBytes(deviceId));
            for (int k = 1; k <= 100; k++) {
                String key = "tag" + k;
                String value = "v" + RandomUtils.nextInt(1, 3);
                put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(key), Bytes.toBytes(value));
            }

            // 将这一条画像数据，添加到list中
            puts.add(put);

            // 攒满100条一批
            if (puts.size() == 100) {
                table.put(puts);
                puts.clear();
            }

        }

        // 提交最后一批
        if (puts.size() > 0) table.put(puts);

        conn.close();
    }
}
