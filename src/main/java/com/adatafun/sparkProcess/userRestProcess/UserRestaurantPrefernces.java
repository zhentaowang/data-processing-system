package com.adatafun.sparkProcess.userRestProcess;

import com.adatafun.sparkProcess.conf.ESMysqlSpark;
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
 * Created by yanggf on 2017/9/25.
 */
public class UserRestaurantPrefernces {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf();

        try{
            String table = "restaurant_order_detail2";
            Dataset orderDS = spark.read().jdbc(propMysql.getProperty("url"),table,propMysql);
            Dataset tbOrderDS = spark.read().jdbc(propMysql.getProperty("url"),"tb_order",propMysql);
            Dataset classDS = spark.read().jdbc(propMysql.getProperty("url"),"restaurant_class",propMysql);


            Dataset togetherDS = orderDS.join(tbOrderDS, orderDS.col("fd_code").equalTo(tbOrderDS.col("order_no")),"left_outer");
            Dataset allDS = togetherDS.join(classDS,orderDS.col("fd_class").equalTo(classDS.col("fd_class")));

            Dataset resultNull = allDS.filter("user_id is not null");

            List<Column> listCols = new ArrayList<Column>();
            listCols.add(allDS.col("user_id"));
            listCols.add(allDS.col("fd_cls"));

            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listCols).toSeq();

            Dataset resultDS = resultNull.select(seqCol);
            JavaRDD<Row> rowRDD = resultDS.toJavaRDD().coalesce(10);
            JavaPairRDD<String,Tuple2<Double,Integer>> pairRDD = rowRDD.mapToPair(new PairFunction<Row, String, Tuple2<Double, Integer>>() {
                public Tuple2<String, Tuple2<Double, Integer>> call(Row row) throws Exception {
                    String userId = String.valueOf(row.getAs(0));
                    Double time = Double.parseDouble(row.getString(1));
                    Tuple2<Double, Integer> tpl2 = new Tuple2<Double, Integer>(time, 1);
                    return new Tuple2<String, Tuple2<Double, Integer>>(userId, tpl2);
                }
            });
            JavaPairRDD<String, Tuple2<Double, Integer>> accRDD = pairRDD.reduceByKey(new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
                public Tuple2<Double, Integer> call(Tuple2<Double, Integer> doubleIntegerTuple2, Tuple2<Double, Integer> doubleIntegerTuple22) throws Exception {
                    Double acc = doubleIntegerTuple2._1() + doubleIntegerTuple22._1();
                    Integer time = doubleIntegerTuple2._2() + doubleIntegerTuple22._2();
                    return new Tuple2<Double, Integer>(acc, time);
                }
            });

            JavaRDD<UserTags> restUserRDD = accRDD.map(new Function<Tuple2<String, Tuple2<Double, Integer>>, UserTags>() {
                public UserTags call(Tuple2<String, Tuple2<Double, Integer>> stringTuple2Tuple2) throws Exception {
                    UserTags user = new UserTags();
                    user.setId(stringTuple2Tuple2._1());
                    if (stringTuple2Tuple2._2()._1() / stringTuple2Tuple2._2()._2() >= 0.5){
                        user.setRestaurantPreferences("中餐");
                    } else {
                        user.setRestaurantPreferences("西餐");
                    }
                    return user;
                }
            });

            SQLContext sqlContext = new SQLContext(spark);
            Dataset ds = sqlContext.createDataFrame(restUserRDD, UserTags.class);
            EsSparkSQL.saveToEs(ds, "user/userTags");



        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
