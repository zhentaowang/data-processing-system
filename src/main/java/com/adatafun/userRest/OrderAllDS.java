package com.adatafun.userRest;

import com.adatafun.conf.ESMysqlSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Created by yanggf on 2017/10/12.
 */
public class OrderAllDS {
    public static void main(String[] args) {
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf2();
        String table = "tbd_app_opera_path";
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        String[] chooseCol = new String[8];
        chooseCol[0] = "page_order = 18";
        chooseCol[1] = "page_order = 19";
        chooseCol[2] = "page_order = 20";
        chooseCol[3] = "page_order = 21";
        chooseCol[4] = "page_order = 22";
        chooseCol[5] = "page_order = 23";
        chooseCol[6] = "page_order = 24";
        chooseCol[7] = "page_order = 25";
        Dataset orderDS = spark.read().jdbc(propMysql.getProperty("url"), table, chooseCol, propMysql);
        orderDS.toJavaRDD().saveAsTextFile("hdfs://192.168.1.131:9000/orderPageOne18-25");
    }
}
