package com.adatafun.sparkProcess.model;

/**
 * Created by yanggf on 2017/9/25.
 */
public class UserTags {
    private static final long seraialVersionUID = 13222132L;
    String id;
    String restaurantPreferences;
    String userId;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getRestaurantPreferences() {
        return restaurantPreferences;
    }

    public void setRestaurantPreferences(String restaurantPreferences) {
        this.restaurantPreferences = restaurantPreferences;
    }
}
