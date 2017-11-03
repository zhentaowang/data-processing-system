package com.adatafun.userTags;

import com.adatafun.conf.ESMysqlSpark;
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
 * Created by yanggf on 2017/10/25.
 * /VirtualCard-v6/share/count/vvip
 * /VirtualCard-v6/vvip/index
 * /VirtualCard-v5/vvip/index
 * /VirtualCard-v5/vvip/setTypes
 * /VirtualCard-v5/share/getVvipShare
 */
public class VVIPBrowseNum {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf2();
        try{
            String table = "tbd_url_element";
            Dataset urlDS = spark.read().jdbc(propMysql.getProperty("url"),table,propMysql);
            Dataset filterDS = urlDS.filter("url = '/VirtualCard-v6/share/count/vvip' or url = '/VirtualCard-v6/vvip/index' " +
                    "or url = '/VirtualCard-v5/vvip/index' or url = '/VirtualCard-v5/vvip/setTypes' or url = '/VirtualCard-v5/share/getVvipShare'");
            List<Column> listCols = new ArrayList<Column>();
            listCols.add(filterDS.col("param"));
            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listCols).toSeq();
            Dataset resultDS = filterDS.select(seqCol);
            JavaRDD<Row> rowRDD = resultDS.toJavaRDD().coalesce(30);
            JavaPairRDD<String , Integer> pairRDD = rowRDD.mapToPair(new PairFunction<Row, String, Integer>() {
                public Tuple2< String, Integer> call(Row row) throws Exception {
                    final Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
                    String jsonStr = String.valueOf(row.getAs(0));
                    Type type = new TypeToken<Map<String, String>>(){}.getType();
                    Map<String, String> map = gson.fromJson(jsonStr, type);
                    String userId = map.get("userId");
                    return new Tuple2<String, Integer>(userId, 1);
                }
            });
            JavaPairRDD<String, Integer> pairFilterRDD = pairRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {
                public Boolean call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                    if(Strings.isBlank(stringIntegerTuple2._1())){
                        return false;
                    }
                    return true;
                }
            });
            JavaPairRDD<String, Integer> accRDD = pairFilterRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });
            JavaRDD<UserTags> restUserRDD = accRDD.map(new Function<Tuple2<String, Integer>, UserTags>() {
                public UserTags call(Tuple2<String, Integer> tuple2IntegerTuple2) throws Exception {
                    UserTags user = new UserTags();
                    user.setId(tuple2IntegerTuple2._1());
                    user.setUserId(tuple2IntegerTuple2._1());
                    user.setVVIPBrowseNum(tuple2IntegerTuple2._2());
                    return user;
                }
            });
            SQLContext sqlContext = new SQLContext(spark);
            Dataset ds = sqlContext.createDataFrame(restUserRDD, UserTags.class);
            EsSparkSQL.saveToEs(ds, "user/userTags");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
