/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.DataFormats;

import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.api.java.UDF1;

import java.util.Random;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.DataTypes;
/**
 *
 * @author hduser
 */
public class Test2 {
    UDF1 concatRandom = new UDF1<String, String>() {
        public String call(final String name) throws Exception {

            Random r = new Random();
            
            return name+r.nextInt();
        }
    };
    
    public Test2() {
        SparkHandlers.getSQLContext().udf().register("concatRandom", concatRandom, DataTypes.StringType);
    }
    
    
    public static void main(String[] args) throws IOException {
        Test2 t = new Test2();

        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "chrome", "winxp", "Link"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "chrome", "winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", "safari", "winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "android", "winxp", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> recordsDF = records.toDF();

        recordsDF.withColumn("new_column", callUDF("concatRandom", col("browser"))).show();
    }
}