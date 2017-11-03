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
import scala.Tuple7;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Created by yanggf on 2017/10/20.
 */
public class RestConsumptionNum {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        try{
            String table = "restaurant_order_detail2";
            Properties prop = ESMysqlSpark.getMysqlConf();
            Dataset orderDS = spark.read().jdbc(prop.getProperty("url"),table,prop);
            Dataset tbOrderDS = spark.read().jdbc(prop.getProperty("url"),"tb_order",prop);
            Dataset restaurantDS = spark.read().jdbc(prop.getProperty("url"),"itd_restaurant",prop);
            Dataset restDS = restaurantDS.filter(restaurantDS.col("fd_lg").equalTo("zh-cn"));

            Dataset togetherDS = orderDS.join(restDS, orderDS.col("fd_restaurant_code").equalTo(restDS.col("fd_code")),"left_outer");
            Dataset allDS = togetherDS.join(tbOrderDS, orderDS.col("fd_code").equalTo(tbOrderDS.col("order_no")),"left_outer");

            List<Column> listColumns = new ArrayList<Column>();
            listColumns.add(tbOrderDS.col("user_id"));//用户id
            listColumns.add(orderDS.col("fd_restaurant_code"));//餐馆code
            listColumns.add(orderDS.col("fd_code"));//订单code
            listColumns.add(orderDS.col("ordertime"));//订单时间
            listColumns.add(orderDS.col("cprice"));//订单金额
            listColumns.add(restDS.col("fd_inspection"));//餐馆位置
            listColumns.add(orderDS.col("fd_class"));//餐馆类型

            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listColumns).toSeq();
            Dataset resultDS = allDS.select(seqCol);//裁剪后的数据
            Dataset resultNull = resultDS.na().drop();//null值数据删除掉
            JavaRDD<Row> rowRDD = resultNull.toJavaRDD();
            JavaPairRDD<String, Integer> idRDD = rowRDD.mapToPair(new PairFunction<Row, String, Integer>() {
                public Tuple2<String, Integer> call(Row strTuple7) throws Exception {
                    String userId = String.valueOf(strTuple7.getAs(0));
                    Tuple2<String, Integer> tplPair = new Tuple2<String, Integer>(userId,1);
                    return tplPair;
                }
            });
            JavaPairRDD<String,Integer> numRDD = idRDD.coalesce(8).reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });
            JavaRDD<UserTags> restRDD = numRDD.map(new Function<Tuple2<String, Integer>, UserTags>() {
                public UserTags call(Tuple2<String, Integer> tuple3IntegerTuple2) throws Exception {
                    UserTags rest = new UserTags();
                    rest.setId(tuple3IntegerTuple2._1());
                    rest.setUserId(tuple3IntegerTuple2._1());
                    rest.setRestConsumptionNum(tuple3IntegerTuple2._2());
                    return rest;
                }
            });
            SQLContext context = new SQLContext(spark);
            Dataset ds = context.createDataFrame(restRDD, UserTags.class);
//            numRDD.saveAsTextFile("f:/CONFDKJFKDJFKDJKF");
            EsSparkSQL.saveToEs(ds,"user/userTags");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
