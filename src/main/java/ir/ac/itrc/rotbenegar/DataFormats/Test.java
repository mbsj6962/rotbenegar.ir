/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.DataFormats;

import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;

import java.io.IOException;
import java.util.List;
import java.util.Arrays;
import org.apache.commons.lang.ArrayUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;


public class Test {
    
    
    public static class Number2 {
        private String first;
        
        public Number2(String first){
            this.first = first;
        }
        
        public String getFirst(){
            return first;
        }
        
        public void setFirst(String first){
            this.first = first;
        }
    }
    
    public static class Number extends Number2 {

        private int i;
        private String english;
        private String french;

        public Number(int i, String english, String french, String first) {
            super(first);
            
            this.i = i;
            this.english = english;
            this.french = french;
        }

        public int getI() {
            return i;
        }

        public void setI(int i) {
            this.i = i;
        }

        public String getEnglish() {
            return english;
        }

        public void setEnglish(String english) {
            this.english = english;
        }

        public String getFrench() {
            return french;
        }

        public void setFrench(String french) {
            this.french = french;
        }
    }

    public static void main(String[] args) throws IOException {
        Encoder<Number> numberEncoder = Encoders.bean(Number.class);
        Encoder<Number2> number2Encoder = Encoders.bean(Number2.class);
        
        List<Number> data = Arrays.asList(
                new Number(1, "one", "un", "f1"),
                new Number(2, "two", "deux", "f2"),
                new Number(3, "three", "trois", "f3"));

        Dataset<Number> ds = SparkHandlers.getSparkSession().createDataset(data, numberEncoder);
        
        System.out.println("DS");
        ds.printSchema();
        ds.select(col("english")).show();

        System.out.println("DF");
        Dataset<Row> df = ds.toDF();
        df.printSchema();
        df.show();
        
        System.out.println("DF->DS");
     Dataset<Number> ds2 = new Dataset<Number>(SparkHandlers.getSQLContext(), df.logicalPlan(), numberEncoder);
        ds2.printSchema();
        ds2.show();
        
        System.out.println(ArrayUtils.contains(ds.columns(), "user_ip"));
    }
}
