package com.adatafun.userRest;

import com.adatafun.conf.ESMysqlSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Created by yanggf on 2017/10/16.
 */
public class OrderAllRest {
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
        chooseCol[0] = "page_order >=26 and page_order <= 29";
        chooseCol[1] = "page_order >=30 and page_order <= 33";
        chooseCol[2] = "page_order >=34 and page_order <= 38";
        chooseCol[3] = "page_order >=39 and page_order <= 55";
        chooseCol[4] = "page_order >=56 and page_order <= 70";
        chooseCol[5] = "page_order >=71 and page_order <= 82";
        chooseCol[6] = "page_order >=83 and page_order <= 700";
        chooseCol[7] = "page_order >=701";
        Dataset orderDS = spark.read().jdbc(propMysql.getProperty("url"), table, chooseCol, propMysql);
        orderDS.toJavaRDD().saveAsTextFile("hdfs://192.168.1.131:9000/orderPageOneRest");
    }
}
