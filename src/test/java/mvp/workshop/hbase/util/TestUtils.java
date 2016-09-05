package mvp.workshop.hbase.util;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

/**
 *
 */
public final class TestUtils {

    private TestUtils() {
    }

    public static void constructTestTable(Connection hbaseConnection, String tableName){
        try (Admin admin = hbaseConnection.getAdmin()) {
            HTableDescriptor descr = new HTableDescriptor(TableName.valueOf(tableName));
            descr.addFamily(new HColumnDescriptor("contacts"));
            descr.addFamily(new HColumnDescriptor("accounts"));
            admin.createTable(descr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void dropTestTable(Connection hbaseConnection, String tableName){
        try (Admin admin = hbaseConnection.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(tableName);
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
