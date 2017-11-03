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
 * Created by yanggf on 2017/10/25.
 */
public class CIPUsageCounter {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties prop = ESMysqlSpark.getMysqlConf3();
        try{
            String table = "tb_order_cip";
            Dataset orderDS = spark.read().jdbc(prop.getProperty("url"),table,prop);
            Dataset tbOrderDS = spark.read().jdbc(prop.getProperty("url"),"tb_order",prop);
            Dataset usageDS = spark.read().jdbc(prop.getProperty("url"),"tb_order_cip_record",prop);
            Dataset tbOrderRenameDS = tbOrderDS.withColumnRenamed("order_no", "order_no1");
            Dataset togetherDS = orderDS.join(tbOrderRenameDS, orderDS.col("order_no").equalTo(tbOrderRenameDS.col("order_no1")),"left_outer");
            Dataset allDS = usageDS.join(togetherDS, usageDS.col("order_no").equalTo(togetherDS.col("order_no")),"left_outer");

            List<Column> listColumns = new ArrayList<Column>();
            listColumns.add(tbOrderDS.col("user_id"));//用户id
            listColumns.add(orderDS.col("pro_code"));//餐馆code

            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listColumns).toSeq();
            Dataset resultDS = allDS.select(seqCol);//裁剪后的数据
            Dataset resultNull = resultDS.na().drop();//null值数据删除掉
            JavaRDD<Row> rowRDD = resultNull.toJavaRDD();

            JavaPairRDD<String, Integer> CIPPairRDD = rowRDD.mapToPair(new PairFunction<Row, String, Integer>() {
                public Tuple2<String, Integer> call(Row row) throws Exception {
                    String userId = String.valueOf(row.getAs(0));
                    return new Tuple2<String, Integer>(userId, 1);
                }
            });

            JavaPairRDD< String,Integer > numRDD = CIPPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });
            JavaRDD<UserTags> restRDD = numRDD.map(new Function<Tuple2<String, Integer>, UserTags>() {
                public UserTags call(Tuple2< String, Integer> tuple2IntegerTuple2) throws Exception {
                    UserTags rest = new UserTags();
                    rest.setId(tuple2IntegerTuple2._1());
                    rest.setUserId(tuple2IntegerTuple2._1());
                    rest.setCIPUsageCounter(tuple2IntegerTuple2._2());
                    return rest;
                }
            });
            SQLContext context = new SQLContext(spark);
            Dataset ds = context.createDataFrame(restRDD, UserTags.class);
//            ds.toJavaRDD().saveAsTextFile("f:/CIPIPIPIPIPI");
            EsSparkSQL.saveToEs(ds,"user/userTags");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
