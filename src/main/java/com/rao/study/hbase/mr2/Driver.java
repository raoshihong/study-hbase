package com.rao.study.hbase.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver implements Tool {
    private Configuration configuration;
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(Driver.class);

        //设置InputFormat

        FileInputFormat.setInputPaths(job,new Path(args[1]));

        //设置Mapper
        job.setMapperClass(HDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置reducer
        TableMapReduceUtil.initTableReducerJob(args[0],MyReducer.class,job);

        //执行job
        boolean result = job.waitForCompletion(true);

        return result?0:1;
    }

    public void setConf(Configuration conf) {
        configuration = conf;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {
        try {
            //直接使用HBaseConfiguration
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");//配置HBase的配置信息
            configuration.set("hbase.zookeeper.property.clientPort","2181");
            ToolRunner.run(configuration,new Driver(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
