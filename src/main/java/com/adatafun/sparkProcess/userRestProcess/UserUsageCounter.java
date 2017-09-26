package com.adatafun.sparkProcess.userRestProcess;

import com.adatafun.sparkProcess.conf.ESMysqlSpark;
import com.adatafun.sparkProcess.model.RestaurantUser;
import com.adatafun.sparkProcess.model.UserTags;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by yanggf on 2017/9/26.
 */
public class UserUsageCounter {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf();

        try{
            String table = "td_restaurant_order";
            Dataset orderDS = spark.read().jdbc(propMysql.getProperty("url"),table,propMysql);
            Dataset tbOrderDS = spark.read().jdbc(propMysql.getProperty("url"),"tb_order",propMysql);


            Dataset togetherDS = orderDS.join(tbOrderDS, orderDS.col("fd_code").equalTo(tbOrderDS.col("order_no")),"left_outer");

            Dataset resultNull = togetherDS.filter("user_id is not null");

            List<Column> listCols = new ArrayList<Column>();
            listCols.add(togetherDS.col("user_id"));
            listCols.add(togetherDS.col("fd_restaurant_code"));

            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listCols).toSeq();

            Dataset resultDS = resultNull.select(seqCol);
            JavaRDD<Row> rowRDD = resultDS.toJavaRDD().coalesce(10);
            JavaPairRDD<Tuple2<String,String> , Integer> pairRDD = rowRDD.mapToPair(new PairFunction<Row, Tuple2<String, String>, Integer>() {
                public Tuple2<Tuple2<String, String>, Integer> call(Row row) throws Exception {
                    String userId = String.valueOf(row.getAs(0));
                    String restaurantCode = row.getString(1);
                    Tuple2<String, String> tpl2 = new Tuple2<String, String>(userId, restaurantCode);
                    return new Tuple2<Tuple2<String, String>, Integer>(tpl2, 1);
                }
            });
            JavaPairRDD<Tuple2<String,String> , Integer> accRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });

            JavaRDD<RestaurantUser> restUserRDD = accRDD.map(new Function<Tuple2<Tuple2<String, String>, Integer>, RestaurantUser>() {
                public RestaurantUser call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                    RestaurantUser user = new RestaurantUser();
                    user.setId(tuple2IntegerTuple2._1()._1() + tuple2IntegerTuple2._1()._2());
                    user.setUsageCounter(tuple2IntegerTuple2._2() / 1.0);
                    return user;
                }
            });


            SQLContext sqlContext = new SQLContext(spark);
            Dataset ds = sqlContext.createDataFrame(restUserRDD, RestaurantUser.class);
            ds.toJavaRDD().saveAsTextFile("f:/LLLLLLLLL");
            EsSparkSQL.saveToEs(ds, "user/userRest");


        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
