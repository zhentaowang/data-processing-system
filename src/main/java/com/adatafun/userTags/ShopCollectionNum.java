package com.adatafun.userTags;

import com.adatafun.conf.ESMysqlSpark;
import com.adatafun.model.UserTags;
import org.apache.logging.log4j.util.Strings;
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
 * Created by yanggf on 2017/10/24.
 */
public class ShopCollectionNum {
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
                if (tuple2IntegerTuple2._1()._2().startsWith("F")){
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
        JavaPairRDD<String, Integer> shopRDD = accPairRDD.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                String userId = tuple2IntegerTuple2._1()._1();
                Integer num = tuple2IntegerTuple2._2();
                return new Tuple2<String, Integer>(userId, num);
            }
        });
        JavaPairRDD<String, Integer> shopFilterRDD = shopRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            public Boolean call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                if(Strings.isBlank(stringIntegerTuple2._1())){
                    return false;
                }
                return true;
            }
        });
        JavaPairRDD<String, Integer> shopAccRDD = shopFilterRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaRDD<UserTags> restUserRDD = shopAccRDD.map(new Function<Tuple2<String, Integer>, UserTags>() {
            public UserTags call(Tuple2<String, Integer> tuple2IntegerTuple2) throws Exception {
                UserTags user = new UserTags();
                user.setId(tuple2IntegerTuple2._1());
                user.setUserId(tuple2IntegerTuple2._1());
                user.setShopCollectionNum(tuple2IntegerTuple2._2());
                return user;
            }
        });
        SQLContext sqlContext = new SQLContext(spark);
        Dataset ds = sqlContext.createDataFrame(restUserRDD, UserTags.class);
//            ds.toJavaRDD().saveAsTextFile("f:/FFFFFF");
        EsSparkSQL.saveToEs(ds, "user/userTags");
    }
}
