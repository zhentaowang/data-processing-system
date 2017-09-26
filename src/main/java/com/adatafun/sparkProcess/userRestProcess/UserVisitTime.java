package com.adatafun.sparkProcess.userRestProcess;

import com.adatafun.sparkProcess.conf.ESMysqlSpark;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by yanggf on 2017/9/22.
 */
public class UserVisitTime {
    public static void main(String[] args) {
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf2();
        String table = "tbd_app_opera_path";
        Dataset orderDS = spark.read().jdbc(propMysql.getProperty("url"), table, propMysql);
        Dataset orderFilterDS = orderDS.filter("page_name = 'RestaurantDetailViewController' or page_name = 'NetworkRestaurantActivity'");

        orderFilterDS.persist(StorageLevel.MEMORY_ONLY());

        Dataset orderDS2 = orderDS.withColumnRenamed("page_time","page_time2");
        Dataset linkDS = orderFilterDS.join(orderDS2,orderFilterDS.col("page_parent_id").equalTo(orderDS2.col("id")),"left_outer");

        List<Column> listColumns = new ArrayList<Column>();
        listColumns.add(orderFilterDS.col("user_id"));
        listColumns.add(orderFilterDS.col("page_name"));
        listColumns.add(orderFilterDS.col("page_time"));
        listColumns.add(linkDS.col("page_time2"));
        Seq<Column> seqCol = JavaConversions.asScalaBuffer(listColumns).toSeq();
        Dataset resultDS = linkDS.select(seqCol);//裁剪后的数据

        resultDS.toJavaRDD().saveAsTextFile("f:/VISITTIME");
    }
}
