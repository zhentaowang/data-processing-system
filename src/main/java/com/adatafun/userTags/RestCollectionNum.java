package com.adatafun.userTags;

import com.adatafun.conf.ESMysqlSpark;
import com.adatafun.model.UserTags;
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
 * Created by yanggf on 2017/10/20.
 */
public class RestCollectionNum {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf();
        Dataset collectDS = spark.read().jdbc(propMysql.getProperty("url"),"tb_user_collect",propMysql);
        JavaRDD<Row> collectRowRDD = collectDS.toJavaRDD();
        JavaPairRDD<String, Integer> collectPairRDD = collectRowRDD.mapToPair(new PairFunction<Row,String, Integer>() {
            public Tuple2<String, Integer> call(Row row) throws Exception {
                String str1 = String.valueOf(row.getAs(1));
                return new Tuple2<String, Integer>(str1, 1);
            }
        });
        JavaPairRDD<String, Integer> restCollectPairRDD = collectPairRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            public Boolean call(Tuple2<String, Integer> tuple2IntegerTuple2) throws Exception {
                if (tuple2IntegerTuple2._1().length() != 0){
                    return true;
                }
                return false;
            }
        });
        JavaPairRDD<String, Integer> accPairRDD = restCollectPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        JavaRDD<UserTags> restRDD = accPairRDD.map(new Function<Tuple2<String, Integer>, UserTags>() {
            public UserTags call(Tuple2<String, Integer> tuple2IntegerTuple2) throws Exception {
                UserTags rest = new UserTags();
                rest.setId(tuple2IntegerTuple2._1());
                rest.setUserId(tuple2IntegerTuple2._1());
                rest.setRestCollectionNum(tuple2IntegerTuple2._2());
                return rest;
            }
        });
        SQLContext sqlContext = new SQLContext(spark);
        Dataset ds = sqlContext.createDataFrame(restRDD, UserTags.class);
//        accPairRDD.saveAsTextFile("f:/RRRRRRRRRRRRRRR");
        EsSparkSQL.saveToEs(ds,"user/userTags");
    }
}
