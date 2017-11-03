package com.adatafun.userRest;

import com.adatafun.conf.ESMysqlSpark;
import com.adatafun.model.RestaurantUser;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.Tuple3;

/**
 * Created by yanggf on 2017/10/18.
 */
public class UserBrowseHour {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        JavaRDD<String> textRDD = spark.sparkContext().textFile("hdfs://192.168.1.131:9000/endAllRDD2",16).toJavaRDD();
        JavaRDD<Tuple3<String, String, String>> tpl3RDD = textRDD.map(new Function<String, Tuple3<String, String, String>>() {
            public Tuple3<String, String, String> call(String s) throws Exception {
                String line = s.replace("(","").replace(")","");
                String userId = line.split(",")[0];
                String restaurantCode = line.split(",")[1];
                String browseHour = line.split(",")[2];
                return new Tuple3<String, String, String>(userId, restaurantCode, browseHour);
            }
        });
        JavaRDD<RestaurantUser> restRDD = tpl3RDD.map(new Function<Tuple3<String, String, String>, RestaurantUser>() {
            public RestaurantUser call(Tuple3<String, String, String> stringStringStringTuple3) throws Exception {
                RestaurantUser rest = new RestaurantUser();
                rest.setId(stringStringStringTuple3._1() + stringStringStringTuple3._2());
                rest.setUserId(stringStringStringTuple3._1());
                rest.setRestaurantCode(stringStringStringTuple3._2());
                rest.setBrowseHour( Integer.parseInt(stringStringStringTuple3._3()));
                return rest;
            }
        });
        SQLContext sqlContext = new SQLContext(spark);
        Dataset ds = sqlContext.createDataFrame(restRDD, RestaurantUser.class);
        EsSparkSQL.saveToEs(ds, "user/userRest");
    }
}
