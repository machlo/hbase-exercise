package com.exericse2.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by daniel on 27.03.17.
 */
public class MapReduce {

    static void mapReduce(Admin admin, String inTable, String outTable) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(admin.getConfiguration(), "Map values");
        job.setJarByClass(MapReduce.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(
                TableName.valueOf(inTable),
                scan,
                MyMapper.class,
                null,
                null,
                job);
        TableMapReduceUtil.initTableReducerJob(
                outTable,
                null,
                job);
        job.setNumReduceTasks(0);
        if (!job.waitForCompletion(true)) {
            throw new IOException("error with job!");
        }
    }

    public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put>  {

        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            context.write(row, resultToPut(row,value));
        }

        private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
            Put put = new Put(key.get());
            for (Cell cell : result.rawCells()) {
                String value = new String(CellUtil.cloneValue(cell));
                value = value.toUpperCase();
                put.addColumn(
                        CellUtil.cloneFamily(cell),
                        CellUtil.cloneQualifier(cell),
                        value.getBytes()
                );
            }
            return put;
        }
    }
}
