package com.adatafun.userParking;

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
 * Created by yanggf on 2017/10/25.
 */
public class UserParkingConsumptionNum {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties prop = ESMysqlSpark.getMysqlConf3();
        try{
            String table = "tb_order_parking";
            Dataset orderDS = spark.read().jdbc(prop.getProperty("url"),table,prop);
            Dataset tbOrderDS = spark.read().jdbc(prop.getProperty("url"),"tb_order",prop);

            Dataset togetherDS = orderDS.join(tbOrderDS, orderDS.col("order_no").equalTo(tbOrderDS.col("order_no")),"left_outer");

            List<Column> listColumns = new ArrayList<Column>();
            listColumns.add(tbOrderDS.col("user_id"));//用户id
            listColumns.add(orderDS.col("parking_code"));//code

            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listColumns).toSeq();
            Dataset resultDS = togetherDS.select(seqCol);//裁剪后的数据
            Dataset resultNull = resultDS.na().drop();//null值数据删除掉
            JavaRDD<Row> rowRDD = resultNull.toJavaRDD();
            JavaPairRDD<Tuple2<String, String>, Integer> parkingPairRDD = rowRDD.mapToPair(new PairFunction<Row, Tuple2<String, String>, Integer>() {
                public Tuple2<Tuple2<String, String>, Integer> call(Row row) throws Exception {
                    String userId = String.valueOf(row.getAs(0));
                    String code = row.getAs(1);
                    Tuple2<String, String> tpl2 = new Tuple2<String,String>(userId, code);
                    return new Tuple2<Tuple2<String, String>, Integer>(tpl2, 1);
                }
            });

            JavaPairRDD< Tuple2<String,String>,Integer > numRDD = parkingPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });
            JavaRDD<RestaurantUser> restRDD = numRDD.map(new Function<Tuple2<Tuple2<String, String>, Integer>, RestaurantUser>() {
                public RestaurantUser call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                    RestaurantUser rest = new RestaurantUser();
                    rest.setId(tuple2IntegerTuple2._1()._1() + tuple2IntegerTuple2._1()._2());
                    rest.setUserId(tuple2IntegerTuple2._1()._1());
                    rest.setRestaurantCode(tuple2IntegerTuple2._1()._2());
                    rest.setConsumptionNum(tuple2IntegerTuple2._2());
                    return rest;
                }
            });

            SQLContext context = new SQLContext(spark);
            Dataset ds = context.createDataFrame(restRDD, RestaurantUser.class);
//            ds.toJavaRDD().saveAsTextFile("f:/CIPIPIPIPIPI");
            EsSparkSQL.saveToEs(ds,"user/userParking");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
