package mvp.workshop.hbase.contacts.dao;

import mvp.workshop.hbase.contacts.model.UserContactModel;
import org.apache.commons.el.EqualityOperator;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Moisieienko Valerii on 16.01.2016.
 */
public class UserContactDao {
    private static final byte[] CONTACTS_FAMILY = "contacts".getBytes();

    private static final byte[] MOBILE_COLUMN = "mobile".getBytes();
    private static final byte[] SKYPE_COLUMN = "skype".getBytes();
    private static final byte[] EMAIL_COLUMN = "email".getBytes();
    private Connection connection;
    private TableName tableName;


    public UserContactDao(Connection connection, TableName tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    public boolean save(UserContactModel userContactModel) {
        try (Table table = this.connection.getTable(this.tableName)) {

            Put put = new Put(Bytes.toBytes(userContactModel.getUserId()));

            if (isContactExists(userContactModel.getMobile())) {
                put.addColumn(CONTACTS_FAMILY, MOBILE_COLUMN,
                        Bytes.toBytes(userContactModel.getMobile()));
            }
            if (isContactExists(userContactModel.getSkype())) {
                put.addColumn(CONTACTS_FAMILY, SKYPE_COLUMN,
                        Bytes.toBytes(userContactModel.getSkype()));
            }
            if (isContactExists(userContactModel.getEmail())) {
                put.addColumn(CONTACTS_FAMILY, EMAIL_COLUMN,
                        Bytes.toBytes(userContactModel.getEmail()));
            }

            table.put(put);
            return true;
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean isContactExists(String contact) {
        return contact != null && !contact.isEmpty();
    }

    public UserContactModel getOne(String userId) {
        try (Table table = this.connection.getTable(this.tableName)) {

            Get get = new Get(Bytes.toBytes(userId));
            get.addFamily(CONTACTS_FAMILY);

            Result res = table.get(get);
            if (res != null && !res.isEmpty()) {
                Map<byte[], byte[]> data = res.getFamilyMap(CONTACTS_FAMILY);
                byte[] mobileValue = data.get(MOBILE_COLUMN);
                byte[] skypeValue = data.get(SKYPE_COLUMN);
                byte[] emailValue = data.get(EMAIL_COLUMN);
                return new UserContactModel(userId,
                        parseContactOrGetEmptyString(mobileValue),
                        parseContactOrGetEmptyString(skypeValue),
                        parseContactOrGetEmptyString(emailValue));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private String parseContactOrGetEmptyString(byte[] contact) {
        return contact != null ? Bytes.toString(contact) : "";
    }

    public boolean delete(String userId) {
        try (Table table = this.connection.getTable(this.tableName)) {
            Delete delete = new Delete(Bytes.toBytes(userId));
            delete.addFamily(CONTACTS_FAMILY);
            table.delete(delete);
            return true;
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public long count() {
        long count = 0;
        try (Table table = this.connection.getTable(this.tableName)) {
            try (ResultScanner scanner = table.getScanner(CONTACTS_FAMILY)) {
                for (Result rs = scanner.next(); rs != null; rs = scanner.next()) {
                    count++;
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

    public List<UserContactModel> getAllUsersWithMobile() {
        List<UserContactModel> toSend = new ArrayList<>();
        try (Table table = this.connection.getTable(this.tableName)) {
            Scan scan = new Scan();
            scan.addFamily(CONTACTS_FAMILY);

            QualifierFilter qualifierFilter =
                    new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                                        new BinaryComparator(MOBILE_COLUMN));
            ValueFilter valueFilter =
                    new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL,
                                    new BinaryComparator("".getBytes()));

            FilterList filterList =
                    new FilterList(FilterList.Operator.MUST_PASS_ALL,
                                   qualifierFilter,
                                   valueFilter);

            scan.setFilter(filterList);

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result res = scanner.next(); res != null; res = scanner.next()) {
                    if (res != null && !res.isEmpty()) {
                        Map<byte[], byte[]> data = res.getFamilyMap(CONTACTS_FAMILY);
                        byte[] mobileValue = data.get(MOBILE_COLUMN);
                        byte[] skypeValue = data.get(SKYPE_COLUMN);
                        byte[] emailValue = data.get(EMAIL_COLUMN);
                        toSend.add(new UserContactModel(Bytes.toString(res.getRow()),
                                                        parseContactOrGetEmptyString(mobileValue),
                                                        parseContactOrGetEmptyString(skypeValue),
                                                        parseContactOrGetEmptyString(emailValue)));
                    }
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return toSend;
    }

    public void dropAll() {
        try (Admin admin = this.connection.getAdmin()) {
            HTableDescriptor descr = admin.getTableDescriptor(this.tableName);
            admin.disableTable(this.tableName);
            admin.deleteTable(this.tableName);
            admin.createTable(descr);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
