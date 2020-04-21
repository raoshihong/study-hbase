package com.rao.study.hbase.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * 自定义Mapper读取HDFS文件的数据,并解析转化为Put对象
 */
public class HDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    //每行都调用
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //解析每行数据
        String line = value.toString();
        String[] fields = line.split("\t");
        //获取rowKey
        String rowKey = fields[0];
        //获取其他列
        String name = fields[1];
        String sex = fields[2];

        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();
        immutableBytesWritable.set(Bytes.toBytes(rowKey));


        //创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        byte[] cf = Bytes.toBytes("base_info");
        byte[] cn = Bytes.toBytes("name");
        byte[] val = Bytes.toBytes(name);
        put.addColumn(cf,cn,val);

        cf = Bytes.toBytes("base_info");
        cn = Bytes.toBytes("sex");
        val = Bytes.toBytes(sex);
        put.addColumn(cf,cn,val);

        //写出
        context.write(immutableBytesWritable,put);

    }
}
