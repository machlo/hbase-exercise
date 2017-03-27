package com.exericse2.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

/**
 * Created by daniel on 27.03.17.
 */
public class BulkLoad {
    public static void doBulkLoad(Admin admin, String pathToHFile, String tableName, String family) {
        try {
            Configuration configuration = admin.getConfiguration();
            HBaseConfiguration.addHbaseResources(configuration);
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(configuration);

            Util.createTable(tableName, family, admin);
            TableName table = TableName.valueOf(tableName);
            loadFfiles.doBulkLoad(
                    new Path(pathToHFile),
                    admin,
                    admin.getConnection().getTable(table),
                    admin.getConnection().getRegionLocator(table)
            );
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }
}
