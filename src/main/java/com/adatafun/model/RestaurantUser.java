package com.adatafun.model;

import java.io.Serializable;

/**
 * Created by yanggf on 2017/9/20.
 */
public class RestaurantUser implements Serializable{
    private static final long seraialVersionUID = 132132L;

    private Float averageOrderAmount;
    private Integer collectionNum;
    private Integer commentNum;
    private Integer consumptionNum;
    private boolean isMultitimeConsumption;
    private String perCustomerTransaction;
    private String restaurantCode;
    private String restaurantPreferences;
    private String userId;
    private double usageCounter;
    private Integer browseNum;
    private Integer browseHour;
    private Integer peopleConsumption;
    private Integer boughtNum;
    private String shopType;
    private Integer favorNum;

    public Integer getFavorNum() {
        return favorNum;
    }

    public void setFavorNum(Integer favorNum) {
        this.favorNum = favorNum;
    }

    public Integer getBrowseHour() {
        return browseHour;
    }

    public void setBrowseHour(Integer browseHour) {
        this.browseHour = browseHour;
    }

    public String getShopType() {
        return shopType;
    }

    public void setShopType(String shopType) {
        this.shopType = shopType;
    }

    public Integer getBoughtNum() {
        return boughtNum;
    }

    public void setBoughtNum(Integer boughtNum) {
        this.boughtNum = boughtNum;
    }

    public Integer getPeopleConsumption() {
        return peopleConsumption;
    }

    public void setPeopleConsumption(Integer peopleConsumption) {
        this.peopleConsumption = peopleConsumption;
    }

    public Integer getBrowseNum() {
        return browseNum;
    }

    public void setBrowseNum(Integer browseNum) {
        this.browseNum = browseNum;
    }

    public double getUsageCounter() {
        return usageCounter;
    }

    public void setUsageCounter(double usageCounter) {
        this.usageCounter = usageCounter;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    private String id;

    public static long getSeraialVersionUID() {
        return seraialVersionUID;
    }

    public Float getAverageOrderAmount() {
        return averageOrderAmount;
    }

    public void setAverageOrderAmount(Float averageOrderAmount) {
        this.averageOrderAmount = averageOrderAmount;
    }

    public Integer getCollectionNum() {
        return collectionNum;
    }

    public void setCollectionNum(Integer collectionNum) {
        this.collectionNum = collectionNum;
    }

    public Integer getCommentNum() {
        return commentNum;
    }

    public void setCommentNum(Integer commentNum) {
        this.commentNum = commentNum;
    }

    public Integer getConsumptionNum() {
        return consumptionNum;
    }

    public void setConsumptionNum(Integer consumptionNum) {
        this.consumptionNum = consumptionNum;
    }

    public boolean isMultitimeConsumption() {
        return isMultitimeConsumption;
    }

    public void setMultitimeConsumption(boolean multitimeConsumption) {
        isMultitimeConsumption = multitimeConsumption;
    }

    public String getPerCustomerTransaction() {
        return perCustomerTransaction;
    }

    public void setPerCustomerTransaction(String perCustomerTransaction) {
        this.perCustomerTransaction = perCustomerTransaction;
    }

    public String getRestaurantCode() {
        return restaurantCode;
    }

    public void setRestaurantCode(String restaurantCode) {
        this.restaurantCode = restaurantCode;
    }

    public String getRestaurantPreferences() {
        return restaurantPreferences;
    }

    public void setRestaurantPreferences(String restaurantPreferences) {
        this.restaurantPreferences = restaurantPreferences;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

}
