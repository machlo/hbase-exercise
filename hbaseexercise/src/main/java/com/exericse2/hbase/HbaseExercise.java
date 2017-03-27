package com.exericse2.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HbaseExercise extends Configured implements Tool {
    private static final String IN_TABLE = "inTable";
    private static final String OUT_TABLE = "outTable";
    private static final String TEMP_DIR = "/tmp/hbaseexercise";
    private static final String COLUMN_FAMILY = "text";
    private static final String COLUMN_NAME = "t";

    /**
     * args[0]: input path
     */
    public static void main(String[] args) {
        try {

            int response = ToolRunner.run(HBaseConfiguration.create(), new HbaseExercise(), args);
            if (response == 0) {
                System.out.println("Job is successfully completed...");
            } else {
                System.out.println("Job failed...");
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        int result = 0;
        try (
                Connection conn = ConnectionFactory.createConnection();
        ) {
            Admin admin = conn.getAdmin();
            Configuration configuration = conn.getConfiguration();

            Util.createTable(IN_TABLE, COLUMN_FAMILY, admin);
            TableName table = TableName.valueOf(IN_TABLE);

            configuration.set("hbase.fs.tmp.dir", "/tmp/hbase-staging");
            configuration.set("hbase.table.name", IN_TABLE);
            configuration.set("COLUMN_FAMILY", COLUMN_FAMILY);
            configuration.set("COLUMN_NAME", COLUMN_NAME);

            Job job = Job.getInstance(configuration);
            job.setWorkingDirectory(new Path("/tmp/"));
            job.setJarByClass(HbaseExercise.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapperClass(BulkLoadMapper.class);

            FileInputFormat.addInputPaths(job, args[0]);

            FileSystem.getLocal(getConf()).delete(new Path(TEMP_DIR), true);
            FileOutputFormat.setOutputPath(job, new Path(TEMP_DIR));

            job.setMapOutputValueClass(Put.class);

            RegionLocator regionLocator = conn.getRegionLocator(table);

            HFileOutputFormat2.configureIncrementalLoad(
                    job,
                    conn.getTable(table),
                    regionLocator
            );

            job.waitForCompletion(true);
            if (job.isSuccessful()) {
                BulkLoad.doBulkLoad(admin, TEMP_DIR, IN_TABLE, COLUMN_FAMILY);
                MapReduce.mapReduce(admin, IN_TABLE, OUT_TABLE);
                System.out.println("OK");
            } else {
                result = -1;
            }
        }
        return result;
    }
}