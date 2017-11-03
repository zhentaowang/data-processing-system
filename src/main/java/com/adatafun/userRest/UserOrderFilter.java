package com.adatafun.userRest;

import com.adatafun.conf.ESMysqlSpark;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by yanggf on 2017/9/22.
 */
public class UserOrderFilter {
    public static void main(String[] args) {
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf2();
        String table = "tbd_app_opera_path";
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Dataset orderDS = spark.read().jdbc(propMysql.getProperty("url"), table, "page_order",1,1500,8,propMysql);
        Dataset orderFilterDS = orderDS.filter("page_name = 'RestaurantDetailViewController' or page_name = 'NetworkRestaurantActivity'");

        List<Column> listColumnsDS = new ArrayList<Column>();
        listColumnsDS.add(orderFilterDS.col("id"));
        listColumnsDS.add(orderFilterDS.col("user_id"));
        listColumnsDS.add(orderFilterDS.col("page_time"));
        listColumnsDS.add(orderFilterDS.col("page_parent_id"));
        Seq<Column> seqCol = JavaConversions.asScalaBuffer(listColumnsDS).toSeq();
        Dataset orderFilterSaveDS = orderFilterDS.select(seqCol);//裁剪后的数据
        orderFilterSaveDS.toJavaRDD().saveAsTextFile("hdfs://192.168.1.131:9000/orderFilterDS");
    }
}
