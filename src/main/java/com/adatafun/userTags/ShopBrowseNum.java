package com.adatafun.userTags;

import com.adatafun.conf.ESMysqlSpark;
import com.adatafun.model.RestaurantUser;
import com.adatafun.model.UserTags;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.util.Strings;
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
 * Created by yanggf on 2017/10/24.
 */
public class ShopBrowseNum {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf2();
        ///VirtualCard-v5/shop/detail   /VirtualCard-v5/shop/getNearbyShare
        try{
            String table = "tbd_url_element";
            Dataset urlDS = spark.read().jdbc(propMysql.getProperty("url"),table,propMysql);
            Dataset filterDS = urlDS.filter("url = '/VirtualCard-v5/shop/detail' or url = '/VirtualCard-v5/shop/getNearbyShare'");

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
                    String restaurantCode = map.get("shopCode");
                    Tuple2<String, String> tpl2 = new Tuple2<String, String>(userId, restaurantCode);
                    return new Tuple2<Tuple2<String, String>, Integer>(tpl2, 1);
                }
            });
            JavaPairRDD<Tuple2<String,String> , Integer> accRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });
            JavaPairRDD<String, Integer> shopRDD = accRDD.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Integer>() {
                public Tuple2<String, Integer> call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                    String userId = tuple2IntegerTuple2._1()._1();
                    Integer num = tuple2IntegerTuple2._2();
                    return new Tuple2<String, Integer>(userId, num);
                }
            });
            JavaPairRDD<String, Integer> shopFilterRDD = shopRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {
                public Boolean call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                    if(Strings.isBlank(stringIntegerTuple2._1())){
                        return false;
                    }
                    return true;
                }
            });
            JavaPairRDD<String, Integer> shopAccRDD = shopFilterRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });

            JavaRDD<UserTags> restUserRDD = shopAccRDD.map(new Function<Tuple2<String, Integer>, UserTags>() {
                public UserTags call(Tuple2<String, Integer> tuple2IntegerTuple2) throws Exception {
                    UserTags user = new UserTags();
                    user.setId(tuple2IntegerTuple2._1());
                    user.setUserId(tuple2IntegerTuple2._1());
                    user.setShopBrowseNum(tuple2IntegerTuple2._2());
                    return user;
                }
            });
            SQLContext sqlContext = new SQLContext(spark);
            Dataset ds = sqlContext.createDataFrame(restUserRDD, UserTags.class);
//            ds.toJavaRDD().saveAsTextFile("f:/FFFFFF");
            EsSparkSQL.saveToEs(ds, "user/userTags");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
