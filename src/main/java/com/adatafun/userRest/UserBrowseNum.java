package com.adatafun.userRest;

import com.adatafun.conf.ESMysqlSpark;
import com.adatafun.model.RestaurantUser;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
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

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by yanggf on 2017/9/26.
 */
public class UserBrowseNum {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf2();
        try{
            String table = "tbd_url_element";
            Dataset urlDS = spark.read().jdbc(propMysql.getProperty("url"),table,propMysql);
            Dataset filterDS = urlDS.filter("url = '/VirtualCard-en/restaurant/detail' or url = '/VirtualCard-v5/restaurant/detail' " +
                    "or url = '/VirtualCard-v6/restaurant/detail'");
            List<Column> listCols = new ArrayList<Column>();
            listCols.add(filterDS.col("param"));
            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listCols).toSeq();
            Dataset resultDS = filterDS.select(seqCol);
            JavaRDD<Row> rowRDD = resultDS.toJavaRDD().coalesce(30);
            JavaPairRDD<Tuple2<String,String> , Integer> pairRDD = rowRDD.mapToPair(new PairFunction<Row, Tuple2<String, String>, Integer>() {
                public Tuple2<Tuple2<String, String>, Integer> call(Row row) throws Exception {
                    final Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
                    String jsonStr = String.valueOf(row.getAs(0));
                    Type type = new TypeToken<Map<String, String>>(){}.getType();
                    Map<String, String> map = gson.fromJson(jsonStr, type);
                    String userId = map.get("userId");
                    String restaurantCode = map.get("code");
                    Tuple2<String, String> tpl2 = new Tuple2<String, String>(userId, restaurantCode);
                    return new Tuple2<Tuple2<String, String>, Integer>(tpl2, 1);
                }
            });
            JavaPairRDD<Tuple2<String,String> , Integer> accRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });
            JavaRDD<RestaurantUser> restUserRDD = accRDD.map(new Function<Tuple2<Tuple2<String, String>, Integer>, RestaurantUser>() {
                public RestaurantUser call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                    RestaurantUser user = new RestaurantUser();
                    user.setId(tuple2IntegerTuple2._1()._1() + tuple2IntegerTuple2._1()._2());
                    user.setUserId(tuple2IntegerTuple2._1()._1());
                    user.setRestaurantCode(tuple2IntegerTuple2._1()._2());
                    user.setBrowseNum(tuple2IntegerTuple2._2());
                    return user;
                }
            });
            SQLContext sqlContext = new SQLContext(spark);
            Dataset ds = sqlContext.createDataFrame(restUserRDD, RestaurantUser.class);
            EsSparkSQL.saveToEs(ds, "user/userRest");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
