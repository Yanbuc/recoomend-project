package daomain;

public class RecUser {
    private int userId;
    private String recInfo;

    public RecUser(int userId, String recInfo) {
        this.userId = userId;
        this.recInfo = recInfo;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getRecInfo() {
        return recInfo;
    }

    public void setRecInfo(String recInfo) {
        this.recInfo = recInfo;
    }
}
