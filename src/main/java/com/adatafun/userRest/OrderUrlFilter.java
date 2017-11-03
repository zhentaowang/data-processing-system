package com.adatafun.userRest;

import com.adatafun.conf.ESMysqlSpark;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.lang.reflect.Type;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yanggf on 2017/10/18.
 */
public class OrderUrlFilter {
    public static long strToDateLong(String strDate) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ParsePosition pos = new ParsePosition(0);
        Date strtodate = formatter.parse(strDate, pos);
        return strtodate.getTime();
    }

    public static String getCode(String s){
        final Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
        Type type = new TypeToken<Map<String, String>>(){}.getType();
        Map<String, String> map = gson.fromJson(s, type);
        String restaurantCode = map.get("code");
        return restaurantCode;
    }
    public static void main(String[] args) {
        SparkSession spark = ESMysqlSpark.getSession();
        Properties propMysql = ESMysqlSpark.getMysqlConf2();
        String table = "tbd_url_element";
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Dataset orderDS = spark.read().jdbc(propMysql.getProperty("url"), table, propMysql);
        Dataset orderFilterDS = orderDS.filter("url = '/VirtualCard-en/restaurant/detail' or url = '/VirtualCard-v5/restaurant/detail' " +
                "or url = '/VirtualCard-v6/restaurant/detail'");

        List<Column> listColumnsDS = new ArrayList<Column>();
        listColumnsDS.add(orderFilterDS.col("user_id"));
        listColumnsDS.add(orderFilterDS.col("param"));
        listColumnsDS.add(orderFilterDS.col("begin_date"));
        Seq<Column> seqCol = JavaConversions.asScalaBuffer(listColumnsDS).toSeq();
        Dataset orderFilterSaveDS = orderFilterDS.select(seqCol);//裁剪后的数据
        JavaRDD<Row> rowRDD = orderFilterSaveDS.toJavaRDD();
        JavaRDD<Tuple3<String, String, String>> tpl3RDD = rowRDD.map(new Function<Row, Tuple3<String, String, String>>() {
            public Tuple3<String, String, String> call(Row row) throws Exception {
                String userId = row.getString(0);
                String code = getCode(row.getString(1));
                String time = String.valueOf( strToDateLong(row.getString(2))/1000 );
                return new Tuple3<String, String, String>(userId, code, time);
            }
        });
        JavaRDD<Tuple3<String, String, String>> filterRDD = tpl3RDD.filter(new Function<Tuple3<String, String, String>, Boolean>() {
            public Boolean call(Tuple3<String, String, String> stringStringStringTuple3) throws Exception {
                if ("0".equals(stringStringStringTuple3._1())){
                    return false;
                } else if(stringStringStringTuple3._2() == null || !stringStringStringTuple3._2().startsWith("R")){
                    return false;
                }
                return true;
            }
        });
//        filterRDD.saveAsTextFile("f:/URLFILTERED");

        filterRDD.saveAsTextFile("hdfs://192.168.1.131:9000/UrlFiltered");
    }
}
