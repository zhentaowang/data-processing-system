package com.adatafun.userTags;

import com.adatafun.conf.ESMysqlSpark;
import com.adatafun.model.RestaurantUser;
import com.adatafun.model.UserTags;
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

import java.util.*;

/**
 * Created by yanggf on 2017/10/24.
 */
public class BannerBrowseNum {
    public static String getBannerId(String value){
        Map<String,String> banners = new HashMap<String, String>(12);
        banners.put("51683051A64B2231E0530264A8C0D3BE","https://activity.dragonpass.com.cn/highrail/equity/membershiplistnew");
        banners.put("51D11245FC325F6EE0530264A8C07548","https://activity.dragonpass.com.cn/newestlounge");
        banners.put("597CFC5FA85839E7E0530264A8C0F0E6","http://mp.weixin.qq.com/s/jFI9UNpQhhNrZgOIj15vSQ");
        banners.put("455AC8D410E86594E0530264A8C05B8E","http://img.dragonpass.com.cn/activity/GD_bank");
        banners.put("59FAB1F5A2B059D2E0530264A8C080A5","https://img.dragonpass.com.cn/activity/easternLounge");
        banners.put("56741F60A813112FE0530264A8C0E847","http://activity.dragonpass.com.cn/shop/summer/membership/from");
        banners.put("5A5EC79C48D3224DE0530264A8C035D6","http://img.dragonpass.com.cn/activity/nDay/");
        banners.put("59EFAD543BC50CC9E0530264A8C021D5","http://mp.weixin.qq.com/s/KQSQgR6GBhxrOnJJDupYjQ");
        banners.put("5A72B21112676150E0530264A8C0293E","https://activity.dragonpass.com.cn/aacar/index/v3");
        banners.put("4389DF2BEBB07C22E0530264A8C0E0A3","https://activity.dragonpass.com.cn/lucky/airmealticket/index");
        banners.put("5766C676F7701850E0530164A8C09A5C","http://img.dragonpass.com.cn/activity/HongKongActive/");
        banners.put("57690E6FA0C93694E0530264A8C03810","https://activity.dragonpass.com.cn/wuhan/vvip/index");
        Iterator<Map.Entry<String, String>> it = banners.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry<String, String> entry = it.next();
            if (value.startsWith(entry.getValue())){
                return entry.getKey();
            }
        }
        return "";
    }
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf2();
        try{
            String table = "tbd_visit_url";
            Dataset urlDS = spark.read().jdbc(propMysql.getProperty("url"),table,propMysql);


            List<Column> listCols = new ArrayList<Column>();
            listCols.add(urlDS.col("user_id"));
            listCols.add(urlDS.col("url"));
            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listCols).toSeq();

            Dataset resultDS = urlDS.select(seqCol);
            JavaRDD<Row> rowRDD = resultDS.toJavaRDD().coalesce(8);

            JavaPairRDD<Tuple2<String,String> , Integer> pairRDD = rowRDD.mapToPair(new PairFunction<Row, Tuple2<String, String>, Integer>() {
                public Tuple2<Tuple2<String, String>, Integer> call(Row row) throws Exception {
                    String userId = String.valueOf(row.getAs(0));
                    String bannerId = getBannerId( String.valueOf(row.getAs(1)));
                    Tuple2<String, String> tpl2 = new Tuple2<String, String>(userId, bannerId);
                    return new Tuple2<Tuple2<String, String>, Integer>(tpl2, 1);
                }
            });
            JavaPairRDD<Tuple2<String,String> , Integer> filterRDD = pairRDD.filter(new Function<Tuple2<Tuple2<String, String>, Integer>, Boolean>() {
                public Boolean call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                    if( "".equals(tuple2IntegerTuple2._1._1()) ){
                        return false;
                    } else if ( "".equals(tuple2IntegerTuple2._1._2()) ) {
                        return false;
                    }
                    return true;
                }
            });

            JavaPairRDD<Tuple2<String,String> , Integer> accRDD = filterRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
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
                    user.setBannerBrowseNum(tuple2IntegerTuple2._2());
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
