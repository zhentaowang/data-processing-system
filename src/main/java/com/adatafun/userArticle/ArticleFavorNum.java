package com.adatafun.userArticle;

import com.adatafun.conf.ESMysqlSpark;
import com.adatafun.model.RestaurantUser;
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
 * Created by yanggf on 2017/10/19.
 */
public class ArticleFavorNum {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf3();
        String table = "tbd_lettres_favor";
        Dataset urlDS = spark.read().jdbc(propMysql.getProperty("url"),table,propMysql);
        List<Column> listCols = new ArrayList<Column>();
        listCols.add(urlDS.col("user_id"));
        listCols.add(urlDS.col("lettres_id"));
        Seq<Column> seqCol = JavaConversions.asScalaBuffer(listCols).toSeq();
        Dataset resultDS = urlDS.select(seqCol);
        JavaRDD<Row> rowRDD = resultDS.toJavaRDD().coalesce(8);
        JavaRDD<Row> filterRowRDD = rowRDD.filter(new Function<Row, Boolean>() {
            public Boolean call(Row row) throws Exception {
                if(row.isNullAt(0)){
                    return false;
                }else if(row.isNullAt(1)){
                    return false;
                }
                return true;
            }
        });
        JavaPairRDD<Tuple2<String, String>, Integer> userArticleRDD = filterRowRDD.mapToPair(new PairFunction<Row, Tuple2<String, String>, Integer>() {
            public Tuple2<Tuple2<String, String>, Integer> call(Row row) throws Exception {
                String userId = row.getString(0);
                String restaurantCode = row.getString(1);
                Tuple2<String, String> tpl2 = new Tuple2<String, String>(userId, restaurantCode);
                return new Tuple2<Tuple2<String, String>, Integer>(tpl2, 1);
            }
        });
        JavaPairRDD<Tuple2<String, String>, Integer> accArticleRDD = userArticleRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        JavaRDD<RestaurantUser> restUserRDD = accArticleRDD.map(new Function<Tuple2<Tuple2<String, String>, Integer>, RestaurantUser>() {
            public RestaurantUser call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                RestaurantUser user = new RestaurantUser();
                user.setId(tuple2IntegerTuple2._1()._1() + tuple2IntegerTuple2._1()._2());
                user.setUserId(tuple2IntegerTuple2._1()._1());
                user.setRestaurantCode(tuple2IntegerTuple2._1()._2());
                user.setFavorNum(tuple2IntegerTuple2._2());
                return user;
            }
        });


        SQLContext sqlContext = new SQLContext(spark);
        Dataset ds = sqlContext.createDataFrame(restUserRDD, RestaurantUser.class);
//        ds.toJavaRDD().saveAsTextFile("f://JKJKJKJKJKJKJKK");
        EsSparkSQL.saveToEs(ds, "user/userArticle");
    }
}
