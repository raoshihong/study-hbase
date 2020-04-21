package com.rao.study.hbase.mr1;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

/**
 * 实现Mapper读取HBase数据
 * Mapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT>
 * TableMapper读取HBase数据返回rowkey和Result
 * MyMapper将数据封装，并输出rowkey和Put对象到Reducer中
 *
 */
public class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //  创建Put对象
        Put put = new Put(key.get());//传递rowKey

        //遍历读取到到数据
        for (Cell cell : value.rawCells()) {
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            //判断列蔟和列名,只将匹配到的列的数据进行迁移
            if ("base_info".equals(cf)) {
                String cn = Bytes.toString(CellUtil.cloneQualifier(cell));
                if ("name".equals(cn) || "sex".equals(cn)) {
                    put.add(cell);
                }
            }
        }

        //这里写出,按key创建reducer
        context.write(key,put);
    }
}
