package com.adatafun.userRest;

import com.adatafun.conf.ESMysqlSpark;
import com.adatafun.model.RestaurantUser;
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
 * Created by yanggf on 2017/9/21.
 */
public class UserCollectionNum {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf();

        Dataset collectDS = spark.read().jdbc(propMysql.getProperty("url"),"tb_user_collect",propMysql);

        JavaRDD<Row> collectRowRDD = collectDS.toJavaRDD();
        JavaPairRDD<Tuple2<String,String>, Integer> collectPairRDD = collectRowRDD.mapToPair(new PairFunction<Row, Tuple2<String, String>, Integer>() {
            public Tuple2<Tuple2<String, String>, Integer> call(Row row) throws Exception {
                String str1 = String.valueOf(row.getAs(1));
                String str2 = row.getString(3);
                Tuple2<String, String> tpl2 = new Tuple2<String, String>(str1, str2);
                return new Tuple2<Tuple2<String, String>, Integer>(tpl2, 1);
            }
        });
        JavaPairRDD<Tuple2<String, String>, Integer> restCollectPairRDD = collectPairRDD.filter(new Function<Tuple2<Tuple2<String, String>, Integer>, Boolean>() {
            public Boolean call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                if (tuple2IntegerTuple2._1()._2().startsWith("R")){
                    return true;
                }
                return false;
            }
        });

        JavaPairRDD<Tuple2<String, String>, Integer> accPairRDD = restCollectPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
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
                rest.setCollectionNum(tuple2IntegerTuple2._2());
                return rest;
            }
        });

        SQLContext sqlContext = new SQLContext(spark);
        Dataset ds = sqlContext.createDataFrame(restRDD, RestaurantUser.class);
        //System.out.println(ds.count());
        EsSparkSQL.saveToEs(ds,"user/userRest");
    }
}
