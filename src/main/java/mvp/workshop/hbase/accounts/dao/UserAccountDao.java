package mvp.workshop.hbase.accounts.dao;

import mvp.workshop.hbase.accounts.model.UserAccount;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Moisieienko Valerii on 16.01.2016.
 */
public class UserAccountDao {
    private static final byte[] ACCOUNTS_FAMILY = "accounts".getBytes();
    private static final double PRECISION = 100000d;

    private Connection connection;
    private TableName tableName;

    public UserAccountDao(Connection connection, TableName tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    public boolean save(UserAccount userAccount) {
        try (Table table = this.connection.getTable(this.tableName)) {

            Put put = new Put(Bytes.toBytes(userAccount.getUserId()));
            Map<String, Double> acc = userAccount.getMoney();
            for (Map.Entry<String, Double> entry : acc.entrySet()) {
                put.addColumn(ACCOUNTS_FAMILY, Bytes.toBytes(entry.getKey()), Bytes.toBytes((long) (entry.getValue() * PRECISION)));
            }
            put.setDurability(Durability.ASYNC_WAL);

            table.put(put);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public UserAccount getOne(String userId) {
        try (Table table = this.connection.getTable(this.tableName)) {

            Get get = new Get(Bytes.toBytes(userId));
            get.addFamily(ACCOUNTS_FAMILY);
            Result res = table.get(get);

            if (res != null && !res.isEmpty()) {
                Map<String, Double> accMap = new HashMap<>();
                Map<byte[], byte[]> data = res.getFamilyMap(ACCOUNTS_FAMILY);
                for (Map.Entry<byte[], byte[]> entry : data.entrySet()) {
                    accMap.put(Bytes.toString(entry.getKey()),
                               Bytes.toLong(entry.getValue()) / PRECISION);
                }
                return new UserAccount(userId, accMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean delete(String userId) {
        try (Table table = this.connection.getTable(this.tableName)) {

            Delete delete = new Delete(Bytes.toBytes(userId));
            delete.addFamily(ACCOUNTS_FAMILY);

            table.delete(delete);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public long count() {
        long count = 0;
        try (Table table = this.connection.getTable(this.tableName)) {

            try (ResultScanner scanner = table.getScanner(ACCOUNTS_FAMILY)) {
                for (Result rs = scanner.next(); rs != null; rs = scanner.next()) {
                    count++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

    public double addMoney(String userId, String currency, double addMoney) {
        double res = 0;
        try (Table table = this.connection.getTable(this.tableName)) {

            long tmp = table.incrementColumnValue(
                    Bytes.toBytes(userId),
                    ACCOUNTS_FAMILY, Bytes.toBytes(currency),
                    (long) (addMoney * PRECISION));

            res = tmp / PRECISION;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    public void dropAll() {
        try (Admin admin = this.connection.getAdmin()) {
            HTableDescriptor descr = admin.getTableDescriptor(this.tableName);
            admin.disableTable(this.tableName);
            admin.deleteTable(this.tableName);
            admin.createTable(descr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
