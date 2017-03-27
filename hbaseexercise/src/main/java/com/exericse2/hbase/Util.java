package com.exericse2.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;

/**
 * Created by daniel on 27.03.17.
 */
public class Util {

    static void createTable(String tableName, String family, Admin admin) throws IOException {
        TableName table = TableName.valueOf(tableName);
        if (admin.tableExists(table)) {
            if (admin.isTableEnabled(table)) {
                admin.disableTable(table);
            }
            admin.deleteTable(table);
        }
        HColumnDescriptor column = new HColumnDescriptor(family);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        tableDescriptor.addFamily(column);

        admin.createTable(tableDescriptor);
    }
}