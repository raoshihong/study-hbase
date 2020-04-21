package com.rao.study.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

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

}
