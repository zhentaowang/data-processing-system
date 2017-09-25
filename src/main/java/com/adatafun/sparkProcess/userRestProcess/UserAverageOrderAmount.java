package com.adatafun.sparkProcess.userRestProcess;

import com.adatafun.sparkProcess.conf.ESMysqlSpark;
import com.adatafun.sparkProcess.model.RestaurantUser;
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
 * Created by yanggf on 2017/9/22.
 */
public class UserAverageOrderAmount {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf();

        try{
            String table = "restaurant_order_detail2";
            Dataset orderDS = spark.read().jdbc(propMysql.getProperty("url"),table,propMysql);
            Dataset tbOrderDS = spark.read().jdbc(propMysql.getProperty("url"),"tb_order",propMysql);
            Dataset restaurantDS = spark.read().jdbc(propMysql.getProperty("url"),"itd_restaurant",propMysql);
            Dataset restDS = restaurantDS.filter(restaurantDS.col("fd_lg").equalTo("zh-cn"));

            Dataset togetherDS = orderDS.join(restDS, orderDS.col("fd_restaurant_code").equalTo(restDS.col("fd_code")),"left_outer");
            Dataset allDS = togetherDS.join(tbOrderDS, orderDS.col("fd_code").equalTo(tbOrderDS.col("order_no")),"left_outer");

            List<Column> listColumns = new ArrayList<Column>();
            listColumns.add(tbOrderDS.col("user_id"));//用户id
            listColumns.add(orderDS.col("fd_restaurant_code"));//餐馆code
            listColumns.add(orderDS.col("fd_code"));//订单code
            listColumns.add(orderDS.col("ordertime"));//订单时间
            listColumns.add(orderDS.col("xfprice"));//订单金额
            listColumns.add(restDS.col("fd_inspection"));//餐馆位置
            listColumns.add(orderDS.col("fd_class"));//餐馆类型

            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listColumns).toSeq();
            Dataset resultDS = allDS.select(seqCol);//裁剪后的数据
            Dataset resultNull = resultDS.na().drop();//null值数据删除掉

            JavaRDD<Row> rowRDD = resultNull.toJavaRDD();

            JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Integer>> timePairRDD = rowRDD.mapToPair(new PairFunction<Row, Tuple2<String, String>, Tuple2<Double, Integer>>() {
                public Tuple2<Tuple2<String, String>, Tuple2<Double, Integer>> call(Row row) throws Exception {
                    String str1 = String.valueOf(row.getAs(0));
                    String str2 = row.getAs(1);
                    Double str3 = row.getAs(4);
                    Tuple2<String, String> tpl2 = new Tuple2<String, String>(str1, str2);
                    Tuple2<Double, Integer> tpl22 = new Tuple2<Double, Integer>(str3,1);
                    return new Tuple2<Tuple2<String, String>, Tuple2<Double, Integer>>(tpl2, tpl22);
                }
            });


            JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Integer>> reducePairRDD = timePairRDD.reduceByKey(new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
                public Tuple2<Double, Integer> call(Tuple2<Double, Integer> doubleIntegerTuple2, Tuple2<Double, Integer> doubleIntegerTuple22) throws Exception {
                    Double amount = doubleIntegerTuple2._1() + doubleIntegerTuple22._1();
                    Integer num = doubleIntegerTuple2._2() + doubleIntegerTuple22._2();
                    Tuple2<Double, Integer> tpl2 = new Tuple2<Double, Integer>(amount,num);
                    return tpl2;
                }
            });

            JavaPairRDD<Tuple2<String,String>, Double> resultPairRDD = reducePairRDD.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Tuple2<Double, Integer>>, Tuple2<String, String>, Double>() {
                public Tuple2<Tuple2<String, String>, Double> call(Tuple2<Tuple2<String, String>, Tuple2<Double, Integer>> tuple2Tuple2Tuple2) throws Exception {
                    String str1 = tuple2Tuple2Tuple2._1()._1();
                    String str2 = tuple2Tuple2Tuple2._1()._2();
                    Tuple2<String, String> tpl2 = new Tuple2<String, String>(str1, str2);
                    Double avgAmout = tuple2Tuple2Tuple2._2()._1() / tuple2Tuple2Tuple2._2()._2();
                    return new Tuple2<Tuple2<String, String>, Double>(tpl2, avgAmout);
                }
            });

            JavaRDD<RestaurantUser> restRDD = resultPairRDD.map(new Function<Tuple2<Tuple2<String, String>, Double>, RestaurantUser>() {
                public RestaurantUser call(Tuple2<Tuple2<String, String>, Double> tuple3IntegerTuple2) throws Exception {
                    RestaurantUser rest = new RestaurantUser();
                    rest.setId(tuple3IntegerTuple2._1()._1() + tuple3IntegerTuple2._1()._2());
                    rest.setUserId(tuple3IntegerTuple2._1()._1());
                    rest.setRestaurantCode(tuple3IntegerTuple2._1()._2());
                    rest.setAverageOrderAmount(tuple3IntegerTuple2._2());
                    return rest;
                }
            });

            SQLContext context = new SQLContext(spark);
            Dataset ds = context.createDataFrame(restRDD, RestaurantUser.class);
            EsSparkSQL.saveToEs(ds,"user/userRest");

        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
