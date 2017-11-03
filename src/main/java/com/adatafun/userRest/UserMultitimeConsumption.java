package com.adatafun.userRest;

import com.adatafun.model.RestaurantUser;
import com.adatafun.conf.ESMysqlSpark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by yanggf on 2017/9/22.
 */
//由于boolean类型字段默认值是false，所以最后一步更新这个字段。（其他字段一更新，该字段默认全都是false，被更新）
public class UserMultitimeConsumption {
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
            listColumns.add(orderDS.col("cprice"));//订单金额
            listColumns.add(restDS.col("fd_inspection"));//餐馆位置
            listColumns.add(orderDS.col("fd_class"));//餐馆类型

            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listColumns).toSeq();
            Dataset resultDS = allDS.select(seqCol);//裁剪后的数据
            Dataset resultNull = resultDS.na().drop();//null值数据删除掉

            JavaRDD<Row> rowRDD = resultNull.toJavaRDD();

            JavaPairRDD<Tuple3<String, String, String>, Integer> timePairRDD = rowRDD.mapToPair(new PairFunction<Row, Tuple3<String, String, String>, Integer>() {
                public Tuple2<Tuple3<String, String, String>, Integer> call(Row row) throws Exception {
                    String str1 = String.valueOf(row.getAs(0));
                    String str2 = row.getAs(1);
                    String str3 = String.valueOf( row.getAs(3) ).substring(0,10);
                    Tuple3<String, String, String> tpl3 = new Tuple3<String, String, String>(str1, str2, str3);
                    return new Tuple2<Tuple3<String, String, String>, Integer>(tpl3, 1);
                }
            });

            JavaPairRDD<Tuple3<String, String, String>, Integer> reducePairRDD = timePairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });

            JavaPairRDD<Tuple2<String,String>, Boolean> resultPairRDD = reducePairRDD.mapToPair(new PairFunction<Tuple2<Tuple3<String, String, String>, Integer>, Tuple2<String, String>, Boolean>() {
                public Tuple2<Tuple2<String, String>, Boolean> call(Tuple2<Tuple3<String, String, String>, Integer> tuple3IntegerTuple2) throws Exception {
                    String str1 = tuple3IntegerTuple2._1()._1();
                    String str2 = tuple3IntegerTuple2._1()._2();
                    Tuple2<String, String> tpl2 = new Tuple2<String, String>(str1, str2);
                    Boolean isMulti = false;
                    if (tuple3IntegerTuple2._2() > 1){
                        isMulti = true;
                    }
                    return new Tuple2<Tuple2<String, String>, Boolean>(tpl2, isMulti);
                }
            });

            JavaRDD<RestaurantUser> restRDD = resultPairRDD.map(new Function<Tuple2<Tuple2<String, String>, Boolean>, RestaurantUser>() {
                public RestaurantUser call(Tuple2<Tuple2<String, String>, Boolean> tuple3IntegerTuple2) throws Exception {
                    RestaurantUser rest = new RestaurantUser();
                    rest.setId(tuple3IntegerTuple2._1()._1() + tuple3IntegerTuple2._1()._2());
                    rest.setUserId(tuple3IntegerTuple2._1()._1());
                    rest.setRestaurantCode(tuple3IntegerTuple2._1()._2());
                    rest.setMultitimeConsumption(tuple3IntegerTuple2._2());
                    return rest;
                }
            });

            SQLContext context = new SQLContext(spark);
            Dataset ds = context.createDataFrame(restRDD, RestaurantUser.class);
//            ds.show();
            EsSparkSQL.saveToEs(ds,"user/userRest");

        } catch (Exception e){
            e.printStackTrace();
        }

    }

}
