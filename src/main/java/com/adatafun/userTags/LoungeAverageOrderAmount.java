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
 * Created by yanggf on 2017/10/23.
 */
public class LoungeAverageOrderAmount {
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
            listCols.add(togetherDS.col("point"));

            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listCols).toSeq();

            Dataset resultDS = resultNull.select(seqCol);
            JavaRDD<Row> rowRDD = resultDS.toJavaRDD().coalesce(10);
            JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> pairRDD = rowRDD.mapToPair(new PairFunction<Row, Tuple2<String, String>, Tuple2<Integer, Integer>>() {
                public Tuple2<Tuple2<String, String>, Tuple2<Integer, Integer>> call(Row row) throws Exception {
                    String userId = String.valueOf(row.getAs(0));
                    String codeStr = String.valueOf(row.getAs(1));
                    Integer point = (int) row.getLong(2);
                    Tuple2<String, String> tpl2 = new Tuple2<String, String>(userId, codeStr);
                    Tuple2<Integer, Integer> tpl22 = new Tuple2<Integer, Integer>(point, 1);
                    return new Tuple2<Tuple2<String, String>, Tuple2<Integer, Integer>>(tpl2, tpl22);
                }
            });

            JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> reduceRDD = pairRDD.reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> integerIntegerTuple22) throws Exception {
                    return new Tuple2<Integer, Integer>(integerIntegerTuple2._1() + integerIntegerTuple22._1(),
                            integerIntegerTuple2._2() + integerIntegerTuple22._2());

                }
            });

            JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> resultRDD = reduceRDD.filter(new Function<Tuple2<Tuple2<String, String>, Tuple2<Integer, Integer>>, Boolean>() {
                public Boolean call(Tuple2<Tuple2<String, String>, Tuple2<Integer, Integer>> tuple2IntegerTuple2) throws Exception {
                    if (tuple2IntegerTuple2._1()._2().startsWith("N"))
                        return true;
                    else
                        return false;
                }
            });
            JavaPairRDD<String, Tuple2<Integer, Integer>> averageRDD = resultRDD.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Tuple2<Integer, Integer>>, String, Tuple2<Integer, Integer>>() {
                public Tuple2<String, Tuple2<Integer, Integer>> call(Tuple2<Tuple2<String, String>, Tuple2<Integer, Integer>> tuple2Tuple2Tuple2) throws Exception {
                    String userId = tuple2Tuple2Tuple2._1()._1();
                    Integer all = tuple2Tuple2Tuple2._2()._1();
                    Integer num = tuple2Tuple2Tuple2._2()._2();
                    Tuple2<Integer, Integer> tpl2 = new Tuple2<Integer, Integer>(all, num);
                    return new Tuple2<String, Tuple2<Integer, Integer>>(userId, tpl2);
                }
            });
            JavaPairRDD<String, Tuple2<Integer, Integer>> averageReduceRDD = averageRDD.reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> integerIntegerTuple22) throws Exception {
                    return new Tuple2<Integer, Integer>(
                            integerIntegerTuple2._1() + integerIntegerTuple22._1(),
                            integerIntegerTuple2._2() + integerIntegerTuple22._2()
                    );
                }
            });
            JavaRDD<UserTags> restUserRDD = averageReduceRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, UserTags>() {
                public UserTags call(Tuple2<String, Tuple2<Integer, Integer>> tuple2IntegerTuple2) throws Exception {
                    UserTags user = new UserTags();
                    user.setUserId(tuple2IntegerTuple2._1());
                    user.setId(tuple2IntegerTuple2._1());
                    user.setLoungeAverageOrderAmount(tuple2IntegerTuple2._2()._1() / tuple2IntegerTuple2._2()._2());
                    return user;
                }
            });

            SQLContext sqlContext = new SQLContext(spark);
            Dataset ds = sqlContext.createDataFrame(restUserRDD, UserTags.class);
//            ds.toJavaRDD().saveAsTextFile("f:/LOUNGAVERAGEECOUNTNUM");
            EsSparkSQL.saveToEs(ds, "user/userTags");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
