package com.rao.study.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HBaseTableTest {

    /**
     * 获取Table对象
     * @throws Exception
     */
    @Test
    public void testTable() throws Exception{
        //设置客户端连接配置
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群配置
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        //获取hbase连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //获取table对象
        Table table = connection.getTable(TableName.valueOf("student"));

        System.out.println(table.getName());

        table.close();
        connection.close();
    }

    /**
     * Put新增和修改数据
     * @throws Exception
     */
    @Test
    public void testPut() throws Exception{
        //设置客户端连接配置
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群配置
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        //获取hbase连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //获取table对象
        Table table = connection.getTable(TableName.valueOf("student"));

        Put put = new Put(Bytes.toBytes("10003"));//指明rowKey
        //指明列蔟,列名,值
        put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("name"),Bytes.toBytes("abc"));
        put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"),Bytes.toBytes(10));

        //新增数据
        table.put(put);

        table.close();
        connection.close();
    }

    /**
     * Get操作
     * @throws Exception
     */
    @Test
    public void testGet()throws Exception{
        //设置客户端连接配置
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群配置
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        //获取hbase连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //获取table对象
        Table table = connection.getTable(TableName.valueOf("student"));

        //通过get查询数据
        Get get = new Get(Bytes.toBytes("10003"));//指定rowKey

        //查询的结果是
        // 10003,base_info:name,aaa
        // 10003,base_info:age,10
        Result result = table.get(get);

        for (Cell cell : result.rawCells()) {
            //rowKey是一个,所以直接从result中获取
            System.out.println("rowKey="+ Bytes.toString(result.getRow())
                    + ",cf="+Bytes.toString(CellUtil.cloneFamily(cell))
                    + ",cn="+Bytes.toString(CellUtil.cloneQualifier(cell))
                    + ",value="+Bytes.toString(CellUtil.cloneValue(cell))
                    + ",row="+Bytes.toString(CellUtil.cloneRow(cell)));
        }


        table.close();
        connection.close();
    }

    /**
     * Scan操作
     */
    @Test
    public void testScanner()throws Exception{
        //设置客户端连接配置
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群配置
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        //获取hbase连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //获取table对象
        Table table = connection.getTable(TableName.valueOf("student"));

        //如果不设置，则默认全表扫描
        Scan scan = new Scan();

        //指定只扫描base_info列蔟下的数据
        scan.addFamily(Bytes.toBytes("base_info"));

        //查询结果：
        //10001  column=base_info:name, timestamp=1587026137538, value=ssss
        //10003  column=base_info:age, timestamp=1587027232618, value=\x00\x00\x00\x0A
        //10003  column=base_info:name, timestamp=1587027232618, value=abc
        ResultScanner scanner = table.getScanner(scan);

        //采用迭代器的方式进行查询数据,因为如果全表扫描的话，数据会有点大,为了避免这个问题,采用迭代器游标的方式查询数据
        for (Result result : scanner) {
            //对每个结果的行进行遍历,获取每个Cell中的数据
            for (Cell cell : result.rawCells()) {
                System.out.println("rowKey="+ Bytes.toString(result.getRow())
                        + ",cf="+Bytes.toString(CellUtil.cloneFamily(cell))
                        + ",cn="+Bytes.toString(CellUtil.cloneQualifier(cell))
                        + ",value="+Bytes.toString(CellUtil.cloneValue(cell))
                        + ",row="+Bytes.toString(CellUtil.cloneRow(cell)));
            }
        }

        table.close();
        connection.close();
    }

    /**
     * 扫描指定版本范围的数据
     * @throws Exception
     */
    @Test
    public void testScanner2()throws Exception{
        //设置客户端连接配置
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群配置
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        //获取hbase连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //获取table对象
        Table table = connection.getTable(TableName.valueOf("student"));

        //如果不设置，则默认全表扫描
        Scan scan = new Scan();

        //设置只展示一个版本的数据
        scan.setMaxVersions(1);

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            //对每个结果的行进行遍历,获取每个Cell中的数据
            for (Cell cell : result.rawCells()) {
                System.out.println("rowKey="+ Bytes.toString(result.getRow())
                        + ",cf="+Bytes.toString(CellUtil.cloneFamily(cell))
                        + ",cn="+Bytes.toString(CellUtil.cloneQualifier(cell))
                        + ",value="+Bytes.toString(CellUtil.cloneValue(cell))
                        + ",row="+Bytes.toString(CellUtil.cloneRow(cell)));
            }
        }

        table.close();
        connection.close();
    }

    @Test
    public void testDelete()throws Exception{
        //设置客户端连接配置
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群配置
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        //获取hbase连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //获取table对象
        Table table = connection.getTable(TableName.valueOf("student"));

        //只指定rowKey,相当于执行deleteall命令
        Delete delete = new Delete(Bytes.toBytes("10001"));//指定rowKey
        //指定删除某个列蔟下的数据
        delete.addFamily(Bytes.toBytes("base_info"));

        table.delete(delete);

        table.close();
        connection.close();
    }

    /**
     * 测试过滤器
     * @throws Exception
     */
    @Test
    public void testFilter() throws Exception{
        //设置客户端连接配置
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群配置
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("student"));
        Scan scan = new Scan();
        //指定过滤器,并指定过滤器的表达式规则,比如rowKey=10001,或者如下面表示rowkey包含子串10003的数据都查出来,注意：hbase的rowkey比较是按位比较,从高位开始比
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("10003"));
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("rowKey="+Bytes.toString(CellUtil.cloneRow(cell))+"," +
                        "CF="+Bytes.toString(CellUtil.cloneFamily(cell))+"," +
                        "CN="+Bytes.toString(CellUtil.cloneQualifier(cell))+"," +
                        "value="+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

}
