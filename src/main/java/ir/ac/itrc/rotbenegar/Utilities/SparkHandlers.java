/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Utilities;

import ir.ac.itrc.rotbenegar.Measuring.VisitorByCriterion;
import ir.ac.itrc.rotbenegar.Measuring.VisitorByCountry;
import ir.ac.itrc.rotbenegar.Measuring.Criterion;
import ir.ac.itrc.rotbenegar.Measuring.Measures;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
/**
 *
 * @author hduser
 */
public class SparkHandlers {
//    private static String appName;
//    private static SparkConf conf;
//    private static JavaSparkContext sc;
//    private static SQLContext sqlContext;
//    private static SparkSession spark;
    
//    public static void Start() {        
        private static String appName = "webranking_backend";
    //    private static Class classesToRegister[] = {
    //                VisitorByCriterion.class,
    //                VisitorByCountry.class,
    //                Criterion.class};

        private static SparkConf conf = new SparkConf()
                            .setAppName(appName)
//                            .setMaster("local[8]")
//                            .setMaster("yarn")
                            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                            .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
//                            .set("spark.driver.allowMultipleContexts", "true")
//                            .set("spark.kryo.registrationRequired", "true")
//                            .set("spark.kryo.registrator", classRegistrator.class.getName())
//                            .registerKryoClasses(classesToRegister)
                            ;
    
        private static JavaSparkContext sc = new JavaSparkContext(conf);
        private static SQLContext sqlContext = new SQLContext(sc);

        private static SparkSession spark = SparkSession
                                            .builder()
                                            .appName(appName)
//                                            .master("local[8]")
                                            .config(conf)
                                            .getOrCreate();
//    }

    public static void Stop() {
        sc.stop();
        spark.stop();
    }
    
    public static SQLContext getSQLContext() {
        return sqlContext;
    }
    
    public static SparkSession getSparkSession() {
        return spark;
    }
    
    public static JavaSparkContext getJavaSparkContext() {
        return sc;
    }
    
    public static class classRegistrator implements KryoRegistrator {

        public void registerClasses(Kryo kryo) {
            kryo.register(VisitorByCriterion.class, new FieldSerializer(kryo, VisitorByCriterion.class));
            kryo.register(Criterion.class, new FieldSerializer(kryo, Criterion.class));
        }
    }
}
