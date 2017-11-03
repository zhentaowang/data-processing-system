package com.adatafun.userTags;

import com.adatafun.conf.ESMysqlSpark;
import com.adatafun.model.RestaurantUser;
import com.adatafun.model.UserTags;
import com.oracle.xmlns.internal.webservices.jaxws_databinding.JavaMethod;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Created by yanggf on 2017/10/20.
 */
public class RestBrowseHour {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        JavaRDD<String> textRDD = spark.sparkContext().textFile("hdfs://192.168.1.131:9000/endAllRDD2",16).toJavaRDD();
        JavaPairRDD<String, Integer> paiRDD = textRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String line = s.replace("(","").replace(")","");
                String userId = line.split(",")[0];
                Integer browseHour = Integer.parseInt(line.split(",")[2]);
                return new Tuple2<String, Integer>(userId, browseHour);
            }
        });
        JavaPairRDD<String, Integer> accRDD = paiRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        JavaRDD<UserTags> restRDD = accRDD.map(new Function<Tuple2<String, Integer>, UserTags>() {
            public UserTags call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                UserTags userTags = new UserTags();
                userTags.setId(stringIntegerTuple2._1());
                userTags.setUserId(stringIntegerTuple2._1());
                userTags.setRestBrowseHour(stringIntegerTuple2._2());
                return userTags;
            }
        });
        SQLContext sqlContext = new SQLContext(spark);
        Dataset ds = sqlContext.createDataFrame(restRDD, UserTags.class);
//        accRDD.saveAsTextFile("f:/BRPWSERSFDFD");
        EsSparkSQL.saveToEs(ds, "user/userTags");
    }
}
