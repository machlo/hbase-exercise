package com.exericse2.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by daniel on 27.03.17.
 */
public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private static long counter = 0;
    private String hbaseTable;
    private String columnFamily;
    private String columnName;
    private ImmutableBytesWritable hbaseTableName;

    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
        hbaseTable = configuration.get("hbase.table.name");
        columnFamily = configuration.get("COLUMN_FAMILY");
        columnName = configuration.get("COLUMN_NAME");
        hbaseTableName = new ImmutableBytesWritable(Bytes.toBytes(hbaseTable));
    }

    public void map(LongWritable key, Text value, Context context) {
        try {
            Put put = new Put(Bytes.toBytes(Long.toString(counter++)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), value.getBytes());
            context.write(hbaseTableName, put);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
