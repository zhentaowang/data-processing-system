package com.adatafun.restaurant;

import com.adatafun.conf.ESMysqlSpark;
import com.adatafun.model.Restaurant;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.Tuple4;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.*;

/**
 * Created by yanggf on 2017/10/19.
 */
public class RestaurantPriority {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf3();
        try{
            String table = "itd_restaurant";
            Dataset urlDS = spark.read().jdbc(propMysql.getProperty("url"),table,propMysql);
            List<Column> listCols = new ArrayList<Column>();
            listCols.add(urlDS.col("fd_code"));
            listCols.add(urlDS.col("fd_lg"));
            listCols.add(urlDS.col("fd_iscoupon"));
            listCols.add(urlDS.col("fd_isflashsale"));
            listCols.add(urlDS.col("fd_settlementdiscount"));
            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listCols).toSeq();
            Dataset resultDS = urlDS.select(seqCol);
            JavaRDD<Row> rowRDD = resultDS.toJavaRDD().coalesce(8);
            JavaRDD<Row> zhcnRowRDD = rowRDD.filter(new Function<Row, Boolean>() {
                public Boolean call(Row row) throws Exception {
                    if ("zh-cn".equals(row.getString(1))){
                        return true;
                    }
                    return false;
                }
            });
            JavaRDD<Tuple4<String,String,String,String>> priorityRDD = zhcnRowRDD.map(new Function<Row, Tuple4<String, String, String, String>>() {
                public Tuple4<String, String, String, String> call(Row row) throws Exception {
                    String restaurantCode = row.getString(0);
                    String isCoupon = null;
                    String isFlash = null;
                    String isDiscount = null;

                    if(row.isNullAt(2)){
                        isCoupon = "0";
                    } else {
                        isCoupon = String.valueOf(row.getLong(2));
                    }
                    if(row.isNullAt(3)){
                        isFlash = "0";
                    } else {
                        isFlash = String.valueOf(row.getLong(3));
                    }
                    if(row.isNullAt(4)){
                        isDiscount = "0";
                    } else {
                        isDiscount = String.valueOf(row.getLong(4));
                    }
                    return new Tuple4<String, String, String, String>(restaurantCode,isCoupon,isFlash,isDiscount);
                }
            });
            JavaRDD<Restaurant> restUserRDD = priorityRDD.map(new Function<Tuple4<String, String, String, String>, Restaurant>() {
                public Restaurant call(Tuple4<String, String, String, String> stringStringStringStringTuple4) throws Exception {
                    Restaurant rest = new Restaurant();
                    rest.setId(stringStringStringStringTuple4._1());
                    rest.setRestaurantCode(stringStringStringStringTuple4._1());
                    if ("1".equals(stringStringStringStringTuple4._3())){
                        rest.setPriority(3);
                    } else if("1".equals(stringStringStringStringTuple4._2())){
                        rest.setPriority(2);
                    } else if("1".equals(stringStringStringStringTuple4._4())){
                        rest.setPriority(1);
                    } else{
                        rest.setPriority(0);
                    }
                    return rest;
                }
            });

            SQLContext sqlContext = new SQLContext(spark);
            Dataset ds = sqlContext.createDataFrame(restUserRDD, Restaurant.class);
//            ds.toJavaRDD().saveAsTextFile("f:/banner");
            EsSparkSQL.saveToEs(ds, "user/restaurant");
        } catch (Exception e){
            e.printStackTrace();
        }

    }

}
