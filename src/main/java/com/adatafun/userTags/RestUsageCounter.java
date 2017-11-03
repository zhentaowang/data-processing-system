package com.adatafun.userTags;

import com.adatafun.conf.ESMysqlSpark;
import com.adatafun.model.RestaurantUser;
import com.adatafun.model.UserTags;
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
 * Created by yanggf on 2017/10/20.
 */
public class RestUsageCounter {
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
            JavaPairRDD<String, Integer> pairRDD = rowRDD.mapToPair(new PairFunction<Row, String, Integer>() {
                public Tuple2<String, Integer> call(Row row) throws Exception {
                    String userId = String.valueOf(row.getAs(0));
                    return new Tuple2<String, Integer>(userId, 1);
                }
            });
            JavaPairRDD<String, Integer> accRDD = pairRDD.coalesce(4).reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });
            JavaRDD<UserTags> restUserRDD = accRDD.map(new Function<Tuple2<String, Integer>, UserTags>() {
                public UserTags call(Tuple2<String, Integer> tuple2IntegerTuple2) throws Exception {
                    UserTags user = new UserTags();
                    user.setId(tuple2IntegerTuple2._1());
                    user.setUserId(tuple2IntegerTuple2._1());
                    user.setRestUsageCounter(tuple2IntegerTuple2._2());
                    return user;
                }
            });
            SQLContext sqlContext = new SQLContext(spark);
            Dataset ds = sqlContext.createDataFrame(restUserRDD, UserTags.class);
            EsSparkSQL.saveToEs(ds, "user/userTags");
//            accRDD.saveAsTextFile("f:/CCCCCCCCCCCCCCC");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
