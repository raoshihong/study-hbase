package com.rao.study.hbase.mr1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;

/**
 * 在Reducer端,通过TableReducer将Put对象写到hbase中
 * Reducer<KEYIN, VALUEIN, KEYOUT, Mutation>
 *
 */
public class MyReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        //遍历将put写到hbase中
        for (Put value : values) {
            context.write(NullWritable.get(),value);
        }
    }
}
