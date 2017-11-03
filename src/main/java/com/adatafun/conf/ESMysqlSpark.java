package com.adatafun.conf;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Created by yanggf on 2017/9/21.
 */
public class ESMysqlSpark {
    public static SparkSession getSession(){
        SparkConf conf = new SparkConf().setAppName("testEs").setMaster("local[*]").set("spark.local.dir", "f:/sparkTmp");
//        SparkConf conf = new SparkConf().setAppName("longtengProcess").setMaster("spark://192.168.1.131:7077");
        Properties props = ApplicationProperty.getInstance().getProperty();
        conf.set("es.index.auto.create",props.getProperty("es.index.auto.create"));
        conf.set("es.resource",props.getProperty("es.resource"));
        conf.set("es.nodes",props.getProperty("es.nodes"));
        conf.set("es.port",props.getProperty("es.port"));
        conf.set("es.nodes.wan.only",props.getProperty("es.nodes.wan.only"));
        conf.set("es.mapping.id",props.getProperty("es.mapping.id"));
        conf.set("es.write.operation",props.getProperty("es.write.operation"));
        conf.set("spark.sql.crossJoin.enabled","true");
//        conf.set("spark.executor.memory", "14g");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config(conf)
                .getOrCreate();
        return spark;
    }
    public static Properties getMysqlConf(){
        Properties appConf = ApplicationProperty.getInstance().getProperty();
        Properties propMysql = new Properties();
        propMysql.setProperty("url",appConf.getProperty("mysqlUrl"));
        propMysql.setProperty("user",appConf.getProperty("user"));
        propMysql.setProperty("password",appConf.getProperty("password"));
        propMysql.setProperty("driver","com.mysql.jdbc.Driver");
        return propMysql;
    }

    public static Properties getMysqlConf2(){
        Properties appConf = ApplicationProperty.getInstance().getProperty();
        Properties propMysql = new Properties();
        propMysql.setProperty("url",appConf.getProperty("mysqlUrl2"));
        propMysql.setProperty("user",appConf.getProperty("user2"));
        propMysql.setProperty("password",appConf.getProperty("password2"));
        propMysql.setProperty("driver","com.mysql.jdbc.Driver");
        return propMysql;
    }

    public static Properties getMysqlConf3(){
        Properties appConf = ApplicationProperty.getInstance().getProperty();
        Properties propMysql = new Properties();
        propMysql.setProperty("url",appConf.getProperty("mysqlUrl3"));
        propMysql.setProperty("user",appConf.getProperty("user3"));
        propMysql.setProperty("password",appConf.getProperty("password3"));
        propMysql.setProperty("driver","com.mysql.jdbc.Driver");
        return propMysql;
    }
}
