package mvp.workshop.hbase.contacts.dao;

import mvp.workshop.hbase.contacts.model.UserContactModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Created by Moisieienko Valerii on 16.01.2016.
 */
public class UserContactDaoTest {
    private static Connection conn;
    private String tableName;
    private UserContactDao dao;
    private List<String> ids;

    @BeforeClass
    public static void init() throws IOException {
        try (InputStream is = UserContactDaoTest.class.getClassLoader().getResourceAsStream("hbase.properties")) {
            Properties hbaseProperties = new Properties();
            hbaseProperties.load(is);
            Configuration conf = new Configuration();
            String zooQuorum = hbaseProperties.getProperty("zookeeper.quorum");
            String zooPort = hbaseProperties.getProperty("zookeeper.port");
            conf.set(HConstants.ZOOKEEPER_QUORUM, zooQuorum);
            conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, zooPort);
            conn = ConnectionFactory.createConnection(conf);
        }
    }

    private void constructTable(String tableName){
        try (Admin admin = conn.getAdmin()) {
            HTableDescriptor descr = new HTableDescriptor(TableName.valueOf(tableName));
            descr.addFamily(new HColumnDescriptor("contacts"));
            descr.addFamily(new HColumnDescriptor("accounts"));
            admin.createTable(descr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void dropTable(String tableName){
        try (Admin admin = conn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(tableName);
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws Exception {
        tableName = "user_accounts_" + UUID.randomUUID().toString();
        constructTable(tableName);
        dao = new UserContactDao(conn, TableName.valueOf(tableName));
        ids = new ArrayList<>();
        String tmpUserId, tmpMobile, tmpSkype, tmpEmail;
        for (int i = 0; i < 100; i++) {
            tmpUserId = UUID.randomUUID().toString();
            tmpMobile = (i % 2 == 0) ? UUID.randomUUID().toString() : "";
            tmpSkype = (i % 4 == 0) ? UUID.randomUUID().toString() : "";
            tmpEmail = UUID.randomUUID().toString();
            ids.add(tmpUserId);
            dao.save(new UserContactModel(tmpUserId, tmpMobile, tmpEmail, tmpSkype));
        }
    }

    @Test
    public void testGetOne() throws Exception {
        for (String userId : ids) {
            assertNotNull(dao.getOne(userId));
        }
    }

    @Test
    public void testDelete() throws Exception {
        for (String userId : ids) {
            dao.delete(userId);
            assertNull(dao.getOne(userId));
        }
    }

    @Test
    public void testDropAll() throws Exception {
        long count = dao.count();
        assertEquals(100, count);
        dao.dropAll();
        count = dao.count();
        assertEquals(0, count);
    }

    @Test
    public void testGetAllUsersWithMobile() {
        List<UserContactModel> mobileContacts = dao.getAllUsersWithMobile();
        assertEquals(50, mobileContacts.size());
    }

    @After
    public void tearDown() throws Exception {
        dropTable(tableName);
    }

    @AfterClass
    public static void shutdown() throws IOException {
        if (conn != null)
            conn.close();
    }
}