package com.adatafun.sparkProcess.userLoungeProcess;

import com.adatafun.sparkProcess.conf.ESMysqlSpark;
import com.adatafun.sparkProcess.model.RestaurantUser;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Created by yanggf on 2017/9/28.
 */
public class LoungeUsageCounter {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf();

        try{
            String table = "tblconsumerecord";
            Dataset orderDS = spark.read().jdbc(propMysql.getProperty("url"),table,propMysql);
            Dataset bindDS = spark.read().jdbc(propMysql.getProperty("url"),"tb_bindrecord",propMysql);


            Dataset togetherDS = orderDS.join(bindDS, orderDS.col("dragoncode").equalTo(bindDS.col("dragoncode")),"left_outer");

            Dataset resultNull = togetherDS.filter("user_id is not null and loungecode is not null");

            List<Column> listCols = new ArrayList<Column>();
            listCols.add(togetherDS.col("user_id"));
            listCols.add(togetherDS.col("loungecode"));
            listCols.add(togetherDS.col("personcount"));

            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listCols).toSeq();

            Dataset resultDS = resultNull.select(seqCol);
            JavaRDD<Row> rowRDD = resultDS.toJavaRDD().coalesce(10);
            JavaPairRDD<Tuple2<String, String>, Integer> pairRDD = rowRDD.mapToPair(new PairFunction<Row, Tuple2<String, String>, Integer>() {
                public Tuple2<Tuple2<String, String>, Integer> call(Row row) throws Exception {
                    String userId = String.valueOf(row.getAs(0));
                    String codeStr = String.valueOf(row.getAs(1));
                    Integer personNum = (int) row.getLong(2);
                    Tuple2<String, String> tpl2 = new Tuple2<String, String>(userId, codeStr);
                    return new Tuple2<Tuple2<String, String>, Integer>(tpl2, personNum);
                }
            });

            JavaPairRDD<Tuple2<String, String>, Integer> reduceRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });

            JavaPairRDD<Tuple2<String, String>, Integer> resultRDD = reduceRDD.filter(new Function<Tuple2<Tuple2<String, String>, Integer>, Boolean>() {
                public Boolean call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                    if (tuple2IntegerTuple2._1()._2().startsWith("N"))
                        return true;
                    else
                        return false;
                }
            });
            JavaRDD<RestaurantUser> restUserRDD = resultRDD.map(new Function<Tuple2<Tuple2<String, String>, Integer>, RestaurantUser>() {
                public RestaurantUser call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                    RestaurantUser user = new RestaurantUser();
                    user.setUserId(tuple2IntegerTuple2._1()._1());
                    user.setRestaurantCode(tuple2IntegerTuple2._1()._2());
                    user.setId(tuple2IntegerTuple2._1()._1() + tuple2IntegerTuple2._1()._2());
                    user.setConsumPersonNum(tuple2IntegerTuple2._2());
                    return user;
                }
            });


            SQLContext sqlContext = new SQLContext(spark);
            Dataset ds = sqlContext.createDataFrame(restUserRDD, RestaurantUser.class);
//            ds.toJavaRDD().saveAsTextFile("f:/LOUNGECOUNTNUM");
            EsSparkSQL.saveToEs(ds, "user/userLounge");


        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
