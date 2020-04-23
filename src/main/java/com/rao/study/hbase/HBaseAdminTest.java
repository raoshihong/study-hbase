package com.rao.study.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.List;

public class HBaseAdminTest {

    /**
     * 获取Admin对象
     * @throws Exception
     */
    @Test
    public void testAdmin() throws Exception{
        Configuration configuration = HBaseConfiguration.create();

        //hbase通过连接到zookeeper就可以与HRegionServer进行通信
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //上面也可以不用指定端口号,因为hbase可以通过以下属性指定端口号
        configuration.set("hbase.zookeeper.property.clientPort","2181");

        Connection conn = ConnectionFactory.createConnection(configuration);
        //hbase通过Admin操作DDL
        Admin admin = conn.getAdmin();

        for (TableName tableName : admin.listTableNames()) {
            //执行list
            System.out.println(Bytes.toString(tableName.getName()));
        }

        admin.close();
        conn.close();
    }

    /**
     * 通过Admin对象创建命名空间
     */
    @Test
    public void testCreateNameSpace()throws Exception{
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群地址
        configuration.set("hbase.zookeeper.quorum","hadoop102:2181,hadoop103:2181,hadoop104:2181");
        Connection conn = ConnectionFactory.createConnection(configuration);

        //获取admin对象
        Admin admin = conn.getAdmin();

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("myspace").build();

        //通过admin对象创建namespace
        admin.createNamespace(namespaceDescriptor);

        admin.close();
        conn.close();
    }

    @Test
    public void testTable()throws Exception{
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群地址
        configuration.set("hbase.zookeeper.quorum","hadoop102:2181,hadoop103:2181,hadoop104:2181");
        Connection conn = ConnectionFactory.createConnection(configuration);

        //获取admin对象
        Admin admin = conn.getAdmin();

        //创建表描述器
        HTableDescriptor tableDescriptors = new HTableDescriptor(TableName.valueOf("student2"));
        //创建表的时候，至少要指定一个列蔟
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("base_info");
        //设置列蔟可存储的版本数
        columnDescriptor.setVersions(1,3);
        tableDescriptors.addFamily(columnDescriptor);
        //创建表
        admin.createTable(tableDescriptors);

        //一定要记得关闭连接
        admin.close();
        conn.close();
    }

    /**
     * 创建表时指定命名空间，并添加多个列蔟
     */
    @Test
    public void testNameSpaceTable()throws Exception{
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群地址
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        Connection conn = ConnectionFactory.createConnection(configuration);

        //获取admin对象
        Admin admin = conn.getAdmin();

        //创建表描述器
        HTableDescriptor tableDescriptors = new HTableDescriptor(TableName.valueOf("myspace:student3"));
        //创建表的时候，至少要指定一个列蔟
        HColumnDescriptor base_info_CF = new HColumnDescriptor("base_info");
        //设置列蔟可存储的版本数
        base_info_CF.setVersions(1,3);
        tableDescriptors.addFamily(base_info_CF);

        //添加第二个列蔟
        HColumnDescriptor address_info_CF = new HColumnDescriptor("address_info");
        tableDescriptors.addFamily(address_info_CF);

        //创建表
        admin.createTable(tableDescriptors);

        //一定要记得关闭连接
        admin.close();
        conn.close();
    }

    /**
     * 删除表
     */
    @Test
    public void testDropTable()throws Exception{
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群地址
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        Connection conn = ConnectionFactory.createConnection(configuration);

        //获取admin对象
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("student2");

        //先判断表是否存储
        boolean exist = admin.tableExists(tableName);

        if (exist) {

            //删除表需要先disable表
            admin.disableTable(tableName);

            //再删除表
            admin.deleteTable(tableName);
        }

        //一定要记得关闭连接
        admin.close();
        conn.close();
    }

    /**
     * 获取表的信息
     */
    @Test
    public void testGetTableInfo()throws Exception{
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群地址
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        Connection conn = ConnectionFactory.createConnection(configuration);

        //获取admin对象
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("student");
        if (!admin.tableExists(tableName)) {
            return;
        }
        HTableDescriptor descriptor = admin.getTableDescriptor(tableName);
        //获取表的列蔟信息
        for (HColumnDescriptor columnFamily : descriptor.getColumnFamilies()) {
            System.out.println("CFName:"+Bytes.toString(columnFamily.getName())+",VERSIONS:"+columnFamily.getMaxVersions());
        }

        //一定要记得关闭连接
        admin.close();
        conn.close();
    }

    /**
     * 修改表
     * @throws Exception
     */
    @Test
    public void testModifyTable()throws Exception{
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群地址
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        Connection conn = ConnectionFactory.createConnection(configuration);

        //获取admin对象
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("student");
        if (!admin.tableExists(tableName)) {
            return;
        }

        //修改表的列蔟信息
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
        for (HColumnDescriptor columnFamily : tableDescriptor.getColumnFamilies()) {
            columnFamily.setVersions(2,10);
        }
        admin.modifyTable(tableName,tableDescriptor);

        //一定要记得关闭连接
        admin.close();
        conn.close();
    }

    /**
     * flush 操作
     * @throws Exception
     */
    @Test
    public void testFlush()throws Exception{
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群地址
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        Connection conn = ConnectionFactory.createConnection(configuration);

        //获取admin对象
        Admin admin = conn.getAdmin();

        admin.flush(TableName.valueOf("student"));

        //一定要记得关闭连接
        admin.close();
        conn.close();
    }

    /**
     * Compact操作
     * @throws Exception
     */
    @Test
    public void testCompact()throws Exception{
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群地址
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        Connection conn = ConnectionFactory.createConnection(configuration);

        //获取admin对象
        Admin admin = conn.getAdmin();

        admin.majorCompact(TableName.valueOf("student"));

        //一定要记得关闭连接
        admin.close();
        conn.close();
    }

    /**
     * 测试预分区,创建表的时候,设置预分区
     */
    @Test
    public void testSplite() throws Exception{
        //设置客户端连接配置
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群配置
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        Connection conn = ConnectionFactory.createConnection(configuration);

        //获取admin对象
        Admin admin = conn.getAdmin();

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("person"));
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("info");
        tableDescriptor.addFamily(columnDescriptor);

        //指定三个分区键,分成4个分区
        byte[][] splits = Bytes.toByteArrays(new String[]{"000|","001|","002|"});

        //指定分区键
        admin.createTable(tableDescriptor,splits);

        //一定要记得关闭连接
        admin.close();
        conn.close();
    }

    /**
     * 测试插入数据到预分区中
     */
    @Test
    public void testPutDataSplit() throws Exception{
        //设置客户端连接配置
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群配置
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        Connection conn = ConnectionFactory.createConnection(configuration);

        Table table = conn.getTable(TableName.valueOf("person"));

        table.put(buildData());

        table.close();
        conn.close();
    }

    /**
     * 构建数据
     * @return
     */
    public List<Put> buildData(){
        List<Put> puts = Lists.newArrayList();

        //按手机号和年-月-日进行hash 求余得出分区号
        String splitKey = "00"+Math.abs("13824411467_2020-04-22".hashCode()%3)+"_";

        Put put = new Put(Bytes.toBytes(splitKey+"13824411467_2020-04-22 12:12:12"));

        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("lisi"));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("count"),Bytes.toBytes("1"));

        puts.add(put);

        Put put1 = new Put(Bytes.toBytes(splitKey+"13824411467_2020-04-22 10:10:12"));

        put1.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("lisi"));
        put1.addColumn(Bytes.toBytes("info"),Bytes.toBytes("count"),Bytes.toBytes("1"));

        puts.add(put1);

        Put put2 = new Put(Bytes.toBytes(splitKey+"13824411467_2020-04-22 10:11:12"));

        put2.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("lisi"));
        put2.addColumn(Bytes.toBytes("info"),Bytes.toBytes("count"),Bytes.toBytes("2"));

        puts.add(put2);

        splitKey = "00"+Math.abs("13824411467_2020-04-23".hashCode()%3)+"_";

        Put put3 = new Put(Bytes.toBytes(splitKey+"13824411467_2020-04-23 10:12:12"));

        put3.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("lisi"));
        put3.addColumn(Bytes.toBytes("info"),Bytes.toBytes("count"),Bytes.toBytes("3"));

        puts.add(put3);

        return puts;
    }

    /**
     * 测试从分区中查数据
     */
    @Test
    public void testScanSplit()throws Exception{
        //设置客户端连接配置
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper集群配置
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        Connection conn = ConnectionFactory.createConnection(configuration);

        Table table = conn.getTable(TableName.valueOf("person"));

        Scan scan = new Scan();

        // 比如要查询手机号13824411467在2020-04-22号到2020-04-23之间的数据
        //查询对应startRowKey,stopRowKey之间的数据
        String splitKey = "00"+Math.abs("13824411467_2020-04-22".hashCode()%3)+"_";


        String startRow = splitKey+"13824411467_2020-04-22 10:11:12";
        String stopRow = splitKey+"13824411467_2020-04-23";
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));

        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result:resultScanner){
            for (Cell cell : result.rawCells()) {
                System.out.println("rowkey="+Bytes.toString(CellUtil.cloneRow(cell)) +
                        ",cf="+Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ",cn="+Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ",value="+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        table.close();
        conn.close();
    }

    @Test
    public void test(){
        System.out.println(Math.abs("13824411467_2020-04-22".hashCode()%3));
        String splitKey = "00"+(Math.abs("13824411467_2020-04-22".hashCode()%3))+"_";
        System.out.println(splitKey);
        splitKey = "00"+"13824411467_2020-04-23".hashCode()%3+"_";
        System.out.println(splitKey);
        splitKey = "00"+Math.abs("13824411467_2020-04-25".hashCode()%3)+"_";
        System.out.println(splitKey);
    }

}
