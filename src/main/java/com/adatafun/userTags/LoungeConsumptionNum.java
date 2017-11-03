package com.adatafun.userTags;

import com.adatafun.conf.ESMysqlSpark;
import com.adatafun.model.RestaurantUser;
import com.adatafun.model.UserTags;
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
 * Created by yanggf on 2017/10/23.
 */
public class LoungeConsumptionNum {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        try{
            Properties prop = ESMysqlSpark.getMysqlConf();
            Dataset orderDS = spark.read().jdbc(prop.getProperty("url"),"tb_order_loungeserv",prop);
            Dataset bindDS = spark.read().jdbc(prop.getProperty("url"),"tb_order",prop);

            Dataset togetherDS = orderDS.join(bindDS, orderDS.col("order_no").equalTo(bindDS.col("order_no")),"left_outer");

            List<Column> listColumns = new ArrayList<Column>();
            listColumns.add(togetherDS.col("user_id"));//用户id
            listColumns.add(orderDS.col("self"));

            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listColumns).toSeq();
            Dataset resultDS = togetherDS.select(seqCol);//裁剪后的数据
            Dataset resultNull = resultDS.na().drop();//null值数据删除掉

            JavaRDD<Row> rowRDD = resultNull.toJavaRDD();

            JavaPairRDD<Tuple2<String, String>, Integer> pairRDD = rowRDD.flatMapToPair(new PairFlatMapFunction<Row, Tuple2<String, String>, Integer>() {
                public Iterator<Tuple2<Tuple2<String, String>, Integer>> call(Row row) throws Exception {
                    ArrayList<Tuple2<Tuple2<String, String>, Integer>> iterList = new ArrayList<Tuple2<Tuple2<String, String>, Integer>>();
                    String userId = String.valueOf(row.getAs(0));
                    String codeStr = String.valueOf(row.getAs(1));
                    String[] codeList = codeStr.split(",");
                    for(String code : codeList){
                        Tuple2<String, String> tpl2 = new Tuple2<String, String>(userId, code);
                        iterList.add(new Tuple2<Tuple2<String, String>, Integer>(tpl2, 1));
                    }
                    return iterList.iterator();
                }
            });
            JavaPairRDD<String, Integer> loungeRDD = pairRDD.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Integer>() {
                public Tuple2<String, Integer> call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                    String userId = tuple2IntegerTuple2._1()._1();
                    Integer num = tuple2IntegerTuple2._2();
                    return new Tuple2<String, Integer>(userId, num);
                }
            });

            JavaPairRDD<String, Integer> reduceRDD = loungeRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });
            JavaRDD<UserTags> restRDD = reduceRDD.map(new Function<Tuple2<String, Integer>, UserTags>() {
                public UserTags call(Tuple2<String, Integer> tuple2IntegerTuple2) throws Exception {
                    UserTags rest = new UserTags();
                    rest.setId(tuple2IntegerTuple2._1());
                    rest.setUserId(tuple2IntegerTuple2._1());
                    rest.setLoungeConsumptionNum(tuple2IntegerTuple2._2());
                    return rest;
                }
            });
            SQLContext context = new SQLContext(spark);
            Dataset ds = context.createDataFrame(restRDD, UserTags.class);
//            ds.toJavaRDD().saveAsTextFile("f:/LOUNGENUMCONSUME");
            EsSparkSQL.saveToEs(ds,"user/userTags");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
