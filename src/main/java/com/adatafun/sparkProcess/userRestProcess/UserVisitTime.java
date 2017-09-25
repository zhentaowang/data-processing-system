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
        orderDS.persist(StorageLevel.MEMORY_AND_DISK());

        Dataset orderDS2 = orderDS.withColumnRenamed("page_time","page_time2");
        Dataset linkDS = orderDS.join(orderDS2,orderDS.col("page_parent_id").equalTo(orderDS2.col("id")),"left_outer");

        List<Column> listColumns = new ArrayList<Column>();
        listColumns.add(orderDS.col("user_id"));
        listColumns.add(orderDS.col("page_name"));
        listColumns.add(orderDS.col("page_time"));
        listColumns.add(linkDS.col("page_time2"));
        Seq<Column> seqCol = JavaConversions.asScalaBuffer(listColumns).toSeq();
        Dataset resultDS = linkDS.select(seqCol);//裁剪后的数据

        resultDS.toJavaRDD().saveAsTextFile("f:/VISITTIME");
    }
}
