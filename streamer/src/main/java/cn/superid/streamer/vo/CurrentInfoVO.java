package cn.superid.streamer.vo;

public class CurrentInfoVO {
    private long onlineUser;
    private long newUser;
    private long activeUser;
    private long totalUser;

    public CurrentInfoVO() {
    }

    public CurrentInfoVO(long onlineUser, long newUser, long activeUser, long totalUser) {
        this.onlineUser = onlineUser;
        this.newUser = newUser;
        this.activeUser = activeUser;
        this.totalUser = totalUser;
    }

    public long getOnlineUser() {
        return onlineUser;
    }

    public void setOnlineUser(long onlineUser) {
        this.onlineUser = onlineUser;
    }

    public long getNewUser() {
        return newUser;
    }

    public void setNewUser(long newUser) {
        this.newUser = newUser;
    }

    public long getActiveUser() {
        return activeUser;
    }

    public void setActiveUser(long activeUser) {
        this.activeUser = activeUser;
    }

    public long getTotalUser() {
        return totalUser;
    }

    public void setTotalUser(long totalUser) {
        this.totalUser = totalUser;
    }
}
