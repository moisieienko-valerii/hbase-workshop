package mvp.workshop.hbase.contacts.model;

/**
 * Created by Moisieienko Valerii on 16.01.2016.
 */
public class UserContactModel {
    private String userId;
    private String mobile;
    private String email;
    private String skype;

    public UserContactModel(String userId, String mobile, String email, String skype) {
        this.userId = userId;
        this.mobile = mobile;
        this.email = email;
        this.skype = skype;
    }

    public String getUserId() {
        return userId;
    }

    public String getMobile() {
        return mobile;
    }

    public String getEmail() {
        return email;
    }

    public String getSkype() {
        return skype;
    }
}
