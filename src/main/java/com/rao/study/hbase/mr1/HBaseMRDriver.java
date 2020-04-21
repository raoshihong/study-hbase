package com.rao.study.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseMRDriver implements Tool {

    private Configuration configuration;

    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(configuration);
        //设置Jar
        job.setJarByClass(HBaseMRDriver.class);

        // 设置Mapper类,参数类型,Mapper读取HBase的表
        Scan scan = new Scan();//进行全表扫描
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("student"),scan,MyMapper.class, ImmutableBytesWritable.class, Put.class,job);

        // 设置Reducer
        TableMapReduceUtil.initTableReducerJob("student2",MyReducer.class,job);

        //提交job
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
            ToolRunner.run(new Configuration(),new HBaseMRDriver(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
