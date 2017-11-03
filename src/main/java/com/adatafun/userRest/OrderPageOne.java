package com.adatafun.userRest;

import com.adatafun.conf.ESMysqlSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Created by yanggf on 2017/10/13.
 */
public class OrderPageOne {
    public static void main(String[] args) {
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf2();
        String table = "tbd_app_opera_path";
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        String pageNum = args[0];
        String[] chooseCol = new String[1];
        chooseCol[0] = "page_order = " + pageNum;
        Dataset orderDS = spark.read().jdbc(propMysql.getProperty("url"), table, chooseCol, propMysql);
        orderDS.toJavaRDD().saveAsTextFile("hdfs://192.168.1.131:9000/orderPageOne" + pageNum);
    }
}
