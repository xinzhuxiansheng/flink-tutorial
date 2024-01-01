package com.yzhou.job.pojo;

public class Access {
    private int categoryId;  // 类别id
    private String description; // 商品描述
    private int id; // 商品id
    private String ip; // 访问的ip信息
    private float money;
    private String name;
    private String os;
    private int status;
    private long ts;

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public float getMoney() {
        return money;
    }

    public void setMoney(float money) {
        this.money = money;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Access{" +
                "categoryId=" + categoryId +
                ", description='" + description + '\'' +
                ", id=" + id +
                ", ip='" + ip + '\'' +
                ", money=" + money +
                ", name='" + name + '\'' +
                ", os='" + os + '\'' +
                ", status=" + status +
                ", ts=" + ts +
                '}';
    }
}
