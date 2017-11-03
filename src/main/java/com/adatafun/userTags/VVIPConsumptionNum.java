package com.adatafun.userTags;

import com.adatafun.conf.ESMysqlSpark;
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
public class VVIPConsumptionNum {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties prop = ESMysqlSpark.getMysqlConf3();
        try{
            String table = "tb_order_vvip";
            Dataset orderDS = spark.read().jdbc(prop.getProperty("url"),table,prop);
            Dataset tbOrderDS = spark.read().jdbc(prop.getProperty("url"),"tb_order",prop);

            Dataset togetherDS = orderDS.join(tbOrderDS, orderDS.col("order_no").equalTo(tbOrderDS.col("order_no")),"left_outer");

            List<Column> listColumns = new ArrayList<Column>();
            listColumns.add(tbOrderDS.col("user_id"));//用户id

            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listColumns).toSeq();
            Dataset resultDS = togetherDS.select(seqCol);//裁剪后的数据
            Dataset resultNull = resultDS.na().drop();//null值数据删除掉
            JavaRDD<Row> rowRDD = resultNull.toJavaRDD();

            JavaPairRDD<String, Integer> CIPPairRDD = rowRDD.mapToPair(new PairFunction<Row, String, Integer>() {
                public Tuple2< String, Integer> call(Row row) throws Exception {
                    String userId = String.valueOf(row.getAs(0));
                    return new Tuple2<String, Integer>(userId, 1);
                }
            });

            JavaPairRDD<String,Integer > numRDD = CIPPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });
            JavaRDD<UserTags> restRDD = numRDD.map(new Function<Tuple2<String, Integer>, UserTags>() {
                public UserTags call(Tuple2<String, Integer> tuple2IntegerTuple2) throws Exception {
                    UserTags rest = new UserTags();
                    rest.setId(tuple2IntegerTuple2._1());
                    rest.setUserId(tuple2IntegerTuple2._1());
                    rest.setVVIPConsumptionNum(tuple2IntegerTuple2._2());
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
