package mvp.workshop.hbase.accounts.model;

import java.util.Map;

/**
 * Created by Moisieienko Valerii on 16.01.2016.
 */
public class UserAccount {
    private String userId;
    private Map<String, Double> money;

    public UserAccount(String userId, Map<String, Double> money) {
        this.userId = userId;
        this.money = money;
    }

    public String getUserId() {
        return userId;
    }

    public Map<String, Double> getMoney() {
        return money;
    }
}
