package mvp.workshop.hbase.contacts.dao;

import mvp.workshop.hbase.contacts.model.UserContactModel;
import mvp.workshop.hbase.util.TestUtils;
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

    @Before
    public void setUp() throws Exception {
        this.tableName = "user_accounts_" + UUID.randomUUID().toString();
        TestUtils.constructTestTable(conn, this.tableName);

        this.dao = new UserContactDao(conn, TableName.valueOf(this.tableName));
        this.ids = new ArrayList<>();

        String tmpUserId, tmpMobile, tmpSkype, tmpEmail;
        for (int i = 0; i < 100; i++) {
            tmpUserId = UUID.randomUUID().toString();
            tmpMobile = (i % 2 == 0) ? UUID.randomUUID().toString() : "";
            tmpSkype = (i % 4 == 0) ? UUID.randomUUID().toString() : "";
            tmpEmail = UUID.randomUUID().toString();
            this.ids.add(tmpUserId);
            this.dao.save(new UserContactModel(tmpUserId, tmpMobile, tmpEmail, tmpSkype));
        }
    }

    @Test
    public void testGetOne() throws Exception {
        for (String userId : this.ids) {
            assertNotNull(this.dao.getOne(userId));
        }
    }

    @Test
    public void testDelete() throws Exception {
        for (String userId : this.ids) {
            this.dao.delete(userId);
            assertNull(this.dao.getOne(userId));
        }
    }

    @Test
    public void testDropAll() throws Exception {
        long count = this.dao.count();
        assertEquals(100, count);
        this.dao.dropAll();
        count = this.dao.count();
        assertEquals(0, count);
    }

    @Test
    public void testGetAllUsersWithMobile() {
        List<UserContactModel> mobileContacts = this.dao.getAllUsersWithMobile();
        assertEquals(50, mobileContacts.size());
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.dropTestTable(conn, this.tableName);
    }

    @AfterClass
    public static void shutdown() throws IOException {
        if (conn != null) {
            conn.close();
        }
    }
}