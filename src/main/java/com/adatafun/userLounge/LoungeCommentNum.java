package com.adatafun.userLounge;

import com.adatafun.model.RestaurantUser;
import com.adatafun.conf.ESMysqlSpark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.Tuple2;

import java.util.Properties;

/**
 * Created by yanggf on 2017/9/27.
 */
public class LoungeCommentNum {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf();

        Dataset collectDS = spark.read().jdbc(propMysql.getProperty("url"),"tb_customer_share",propMysql);
        Dataset filterDS = collectDS.filter("user_id is not null and code is not null and score is not null and score > 2");

        JavaRDD<Row> collectRowRDD = filterDS.toJavaRDD();
        JavaPairRDD<Tuple2<String,String>, Integer> collectPairRDD = collectRowRDD.mapToPair(new PairFunction<Row, Tuple2<String, String>, Integer>() {
            public Tuple2<Tuple2<String, String>, Integer> call(Row row) throws Exception {
                String str1 = String.valueOf(row.getAs(6));
                String str2 = row.getString(17);
                Tuple2<String, String> tpl2 = new Tuple2<String, String>(str1, str2);
                return new Tuple2<Tuple2<String, String>, Integer>(tpl2, 1);
            }
        });
        JavaPairRDD<Tuple2<String, String>, Integer> restCommentPairRDD = collectPairRDD.filter(new Function<Tuple2<Tuple2<String, String>, Integer>, Boolean>() {
            public Boolean call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                if (tuple2IntegerTuple2._1()._2() == null){
                    return false;
                } else if (tuple2IntegerTuple2._1()._2().startsWith("N")){
                    return true;
                }
                return false;
            }
        });
        //System.out.println(restCommentPairRDD.count());

        JavaPairRDD<Tuple2<String, String>, Integer> accPairRDD = restCommentPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaRDD<RestaurantUser> restRDD = accPairRDD.map(new Function<Tuple2<Tuple2<String, String>, Integer>, RestaurantUser>() {
            public RestaurantUser call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                RestaurantUser rest = new RestaurantUser();
                rest.setId(tuple2IntegerTuple2._1()._1() + tuple2IntegerTuple2._1()._2());
                rest.setUserId(tuple2IntegerTuple2._1()._1());
                rest.setRestaurantCode(tuple2IntegerTuple2._1()._2());
                rest.setCommentNum(tuple2IntegerTuple2._2());
                return rest;
            }
        });

        SQLContext sqlContext = new SQLContext(spark);
        Dataset ds = sqlContext.createDataFrame(restRDD, RestaurantUser.class);
//        ds.toJavaRDD().saveAsTextFile("f:/HAOPINGCOMMENT");
        EsSparkSQL.saveToEs(ds,"user/userLounge");
    }
}
