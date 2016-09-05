package mvp.workshop.hbase.accounts.dao;

import mvp.workshop.hbase.accounts.model.UserAccount;
import mvp.workshop.hbase.contacts.dao.UserContactDao;
import mvp.workshop.hbase.contacts.model.UserContactModel;
import mvp.workshop.hbase.util.TestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by Moisieienko Valerii on 16.01.2016.
 */
public class UserAccountDaoTest {
    private static Connection conn;
    private String tableName;
    private UserAccountDao dao;
    private List<String> ids;

    @BeforeClass
    public static void init() throws IOException {
        try (InputStream is = UserAccountDaoTest.class.getClassLoader().getResourceAsStream("hbase.properties")) {
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

        this.dao = new UserAccountDao(conn, TableName.valueOf(this.tableName));
        this.ids = new ArrayList<>();

        String tmpUserId;
        for (int i = 0; i < 100; i++) {
            tmpUserId = UUID.randomUUID().toString();
            this.ids.add(tmpUserId);
            this.dao.save(new UserAccount(tmpUserId, generateMoney()));
        }
    }

    private Map<String, Double> generateMoney() {
        Map<String, Double> toSend = new HashMap<>();
        toSend.put("USD", Math.random() * 100000 + 1);
        toSend.put("UAH", Math.random() * 10000000 + 1);
        return toSend;
    }

    @Test
    public void testGetOne() throws Exception {
        UserAccount tmp;
        for (String userId : this.ids) {
            tmp = this.dao.getOne(userId);

            assertNotNull(tmp);

            assertTrue(tmp.getMoney().containsKey("USD"));
            assertTrue(tmp.getMoney().containsKey("UAH"));

            assertTrue(tmp.getMoney().get("USD") > 0);
            assertTrue(tmp.getMoney().get("UAH") > 0);
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
    public void testAddMoney() throws Exception {
        UserAccount tmp;
        double tmpUsd, tmpUsdAdd, tmpRes;
        for (String userId : this.ids) {
            tmp = this.dao.getOne(userId);

            tmpUsd = tmp.getMoney().get("USD");
            tmpUsdAdd = Math.random() * 1000 + 1;
            tmpRes = this.dao.addMoney(userId, "USD", tmpUsdAdd);

            assertEquals(tmpUsd + tmpUsdAdd, tmpRes, 0.5);
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