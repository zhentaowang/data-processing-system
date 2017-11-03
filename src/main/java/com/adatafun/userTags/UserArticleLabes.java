package com.adatafun.userTags;

import com.adatafun.conf.ESMysqlSpark;
import com.adatafun.model.UserTags;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.Tuple2;
import scala.Tuple5;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by yanggf on 2017/10/19.
 */
public class UserArticleLabes {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf2();
        Properties propMysql3 = ESMysqlSpark.getMysqlConf3();
        String table = "tbd_visit_url";
        String table2 = "tbd_belles_lettres";
        Dataset urlDS = spark.read().jdbc(propMysql.getProperty("url"),table,propMysql);
        Dataset lettresDS = spark.read().jdbc(propMysql3.getProperty("url"),table2,propMysql3);
        List<Column> listCols = new ArrayList<Column>();
        listCols.add(urlDS.col("user_id"));
        listCols.add(urlDS.col("url"));
        Seq<Column> seqCol = JavaConversions.asScalaBuffer(listCols).toSeq();
        Dataset resultDS = urlDS.select(seqCol);
        JavaRDD<Row> rowRDD = resultDS.toJavaRDD().coalesce(2);
        JavaRDD<Row> filterRowRDD = rowRDD.filter(new Function<Row, Boolean>() {
            public Boolean call(Row row) throws Exception {
                if(row.isNullAt(0)){
                    return false;
                }else if(!row.getString(1).startsWith("https://m.dragonpass.com.cn/institute/")){
                    return false;
                }
                return true;
            }
        });
        List<Column> listCols2 = new ArrayList<Column>();
        listCols2.add(lettresDS.col("id"));
        listCols2.add(lettresDS.col("person_labels"));
        Seq<Column> seqCol2 = JavaConversions.asScalaBuffer(listCols2).toSeq();
        Dataset lettresRowDS = lettresDS.select(seqCol2);
        JavaRDD<Row> lettresRowRDD = lettresRowDS.toJavaRDD();

        JavaPairRDD<String, Tuple2<String, Integer>> userArticleRDD = filterRowRDD.mapToPair(new PairFunction<Row, String, Tuple2<String, Integer>>() {
            public Tuple2<String, Tuple2<String, Integer>> call(Row row) throws Exception {
                String userId = row.getString(0);
                String url = row.getString(1);
                String restaurantCode = url.split("/")[4].split("\\?")[0];
                Tuple2<String, Integer> tpl2 = new Tuple2<String, Integer>(userId, 1);
                return new Tuple2<String, Tuple2<String, Integer>>(restaurantCode, tpl2);
            }
        });
        JavaRDD<Row> filterLettresRDD = lettresRowRDD.filter(new Function<Row, Boolean>() {
            public Boolean call(Row row) throws Exception {
                if(row.isNullAt(1))
                    return false;
                return true;
            }
        });
        JavaPairRDD<String,String> lettresRDD = filterLettresRDD.mapToPair(new PairFunction<Row, String, String>() {
            public Tuple2<String, String> call(Row row) throws Exception {
                String restaurantCode = row.getString(0);
                String labels = row.getString(1);
                return new Tuple2<String, String>(restaurantCode, labels);
            }
        });
        JavaPairRDD<String, Tuple2<Tuple2<String,Integer>, Optional<String>>> joinRDD = userArticleRDD.leftOuterJoin(lettresRDD);
        JavaPairRDD<String, String> userLabelsRDD = joinRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<String, Integer>, Optional<String>>>, String, String>() {
            public Tuple2<String, String> call(Tuple2<String, Tuple2<Tuple2<String, Integer>, Optional<String>>> stringTuple2Tuple2) throws Exception {
                String userId = stringTuple2Tuple2._2()._1()._1();
                String labels = stringTuple2Tuple2._2()._2().orElse("0");
                return new Tuple2<String, String>(userId, labels);
            }
        });
        JavaPairRDD<String, Tuple5<Integer,Integer,Integer,Integer,Integer>> labelsNumRDD = userLabelsRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
            public Tuple2<String, Tuple5<Integer, Integer, Integer, Integer, Integer>> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                Integer business = 0;
                Integer student = 0;
                Integer eleven = 0;
                Integer home = 0;
                Integer crowd = 0;
                if(stringStringTuple2._2().contains("商旅出行")){
                    business = 1;
                }else if(stringStringTuple2._2().contains("学生/背包客")){
                    student = 1;
                }else if(stringStringTuple2._2().contains("十一")){
                    eleven = 1;
                }else if(stringStringTuple2._2().contains("家庭出行")){
                    home = 1;
                }else if(stringStringTuple2._2().contains("人群标签")){
                    crowd = 1;
                }
                String userId = stringStringTuple2._1();
                Tuple5<Integer, Integer, Integer, Integer, Integer> tpl5 = new Tuple5<Integer, Integer, Integer, Integer, Integer>(
                        business,student,eleven,home,crowd);
                return new Tuple2<String, Tuple5<Integer, Integer, Integer, Integer, Integer>>(userId,tpl5);
            }
        });
        JavaPairRDD<String, Tuple5<Integer,Integer,Integer,Integer,Integer>> accLablesRDD = labelsNumRDD.reduceByKey(new Function2<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
            public Tuple5<Integer, Integer, Integer, Integer, Integer> call(Tuple5<Integer, Integer, Integer, Integer, Integer> inple5, Tuple5<Integer, Integer, Integer, Integer, Integer> inple52) throws Exception {
                Integer int1 = inple5._1() + inple52._1();
                Integer int2 = inple5._2() + inple52._2();
                Integer int3 = inple5._3() + inple52._3();
                Integer int4 = inple5._4() + inple52._4();
                Integer int5 = inple5._5() + inple52._5();
                return new Tuple5<Integer, Integer, Integer, Integer, Integer>(int1,int2,int3,int4,int5);
            }
        });
        JavaRDD<UserTags> tagsRDD = accLablesRDD.map(new Function<Tuple2<String, Tuple5<Integer, Integer, Integer, Integer, Integer>>, UserTags>() {
            public UserTags call(Tuple2<String, Tuple5<Integer, Integer, Integer, Integer, Integer>> stringTuple5Tuple2) throws Exception {
                UserTags user = new UserTags();
                user.setUserId(stringTuple5Tuple2._1());
                user.setId(stringTuple5Tuple2._1());
                user.setBusiness(stringTuple5Tuple2._2._1());
                user.setStudent(stringTuple5Tuple2._2._2());
                user.setEleven(stringTuple5Tuple2._2._3());
                user.setHome(stringTuple5Tuple2._2._4());
                user.setCrowd(stringTuple5Tuple2._2._5());
                return user;
            }
        });
        SQLContext sqlContext = new SQLContext(spark);
        Dataset ds = sqlContext.createDataFrame(tagsRDD, UserTags.class);
        EsSparkSQL.saveToEs(ds, "user/userTags");
    }

}
