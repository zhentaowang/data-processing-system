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
public class CIPAverageOrderAmount {
    public static void main(String[] args){
        SparkSession spark = ESMysqlSpark.getSession();
        Properties prop = ESMysqlSpark.getMysqlConf3();
        try{
            String table = "tb_order_cip";
            Dataset orderDS = spark.read().jdbc(prop.getProperty("url"),table,prop);
            Dataset tbOrderDS = spark.read().jdbc(prop.getProperty("url"),"tb_order",prop);

            Dataset togetherDS = orderDS.join(tbOrderDS, orderDS.col("order_no").equalTo(tbOrderDS.col("order_no")),"left_outer");

            List<Column> listColumns = new ArrayList<Column>();
            listColumns.add(tbOrderDS.col("user_id"));//用户id
            listColumns.add(orderDS.col("pro_code"));//code
            listColumns.add(tbOrderDS.col("actual_amount"));//消费金额

            Seq<Column> seqCol = JavaConversions.asScalaBuffer(listColumns).toSeq();
            Dataset resultDS = togetherDS.select(seqCol);//裁剪后的数据
            Dataset resultNull = resultDS.na().drop();//null值数据删除掉
            JavaRDD<Row> rowRDD = resultNull.toJavaRDD();

            JavaPairRDD<String, Tuple2<Integer, Integer>> CIPPairRDD = rowRDD.mapToPair(new PairFunction<Row, String, Tuple2<Integer,Integer>>() {
                public Tuple2<String, Tuple2<Integer, Integer>> call(Row row) throws Exception {
                    String userId = String.valueOf(row.getAs(0));
                    Integer amount = (int)row.getDouble(2);
                    Tuple2<Integer, Integer> amountTuple2 = new Tuple2<Integer, Integer>(amount, 1);
                    return new Tuple2<String, Tuple2<Integer, Integer>>(userId, amountTuple2);
                }
            });

            JavaPairRDD<String,Tuple2<Integer, Integer>> numRDD = CIPPairRDD.reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> integerIntegerTuple22) throws Exception {
                    return new Tuple2<Integer, Integer>(
                            integerIntegerTuple2._1() + integerIntegerTuple2._1(),
                            integerIntegerTuple2._2() + integerIntegerTuple2._2()
                    );
                }
            });
            JavaPairRDD<String, Tuple2<Integer, Integer>> numFilterRDD = numRDD.filter(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Boolean>() {
                public Boolean call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {
                    if(stringTuple2Tuple2._2()._2() == 0){
                        return false;
                    }
                    return true;
                }
            });
            JavaRDD<UserTags> restRDD = numFilterRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, UserTags>() {
                public UserTags call(Tuple2<String, Tuple2<Integer, Integer>> tuple2IntegerTuple2) throws Exception {
                    UserTags rest = new UserTags();
                    rest.setId(tuple2IntegerTuple2._1());
                    rest.setUserId(tuple2IntegerTuple2._1());
                    rest.setCIPAverageOrderAmount(tuple2IntegerTuple2._2()._1() / tuple2IntegerTuple2._2()._2());
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
