/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Pipeline;

import ir.ac.itrc.rotbenegar.DataFormats.DatasetFactory;
import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author hhduser
 */
public class LogProcessingTest {

    private static Boolean contentOfDataFramesIsEqual(Dataset<Row> expectedDF, Dataset<Row> testDF) {
        Dataset<Row> df11 = expectedDF.select(col("TimeStamp"), col("user_ip"),
                col("user_ID"), col("destination"), col("day"));

//df11.write().format("com.databricks.spark.csv").option("header", "true").save("expectedOutput");        
        Dataset<Row> df22 = testDF.select(col("TimeStamp"), col("user_ip"),
                col("user_ID"), col("destination"), col("day"));
//df22.write().format("com.databricks.spark.csv").option("header", "true").save("actualOutPut");

        return df11.except(df22).count() == 0 && df22.except(df11).count() == 0;
    }

    public LogProcessingTest() {

    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of isApacheLog method, of class LogProcessing.
     */
    @Test
    public void testClean() {

        //input file --- df
        List<DatasetFactory.outPutOfParse> outputparse = Arrays.asList(
                new DatasetFactory.outPutOfParse("2017-10-18T17:18:47.928041", "10.81.28.192", "94.182.163.19", "static2.farakav.com", "10.81.28.192"),
                new DatasetFactory.outPutOfParse("2017-10-18T17:18:31.661994", "172.25.154.193", "185.49.84.250", "namnak.com", "172.25.154.193"),
                new DatasetFactory.outPutOfParse("2017-10-18T17:16:45.226841", "100.92.218.151", "192.168.194.73", "us01.salmonads.com", "100.92.218.151"),
                new DatasetFactory.outPutOfParse("2017-10-18T17:17:34.028644", "100.82.150.116", "185.147.176.115", "stream1.asset.filimo.com", "100.82.150.116")
        );

        Dataset<DatasetFactory.outPutOfParse> actualoutPutClean
                = SparkHandlers.getSparkSession().createDataset(outputparse, DatasetFactory.getOutPutOfParseEncoder());
        Dataset<Row> actualOutPutCleanDF = actualoutPutClean.toDF();

        LogProcessing instance = new LogProcessing("1");

        Dataset<Row> actualDF = instance.clean(actualOutPutCleanDF);
//        actualDF.show();
        List<DatasetFactory.outPutClean> outputclean = Arrays.asList(
                //               
                new DatasetFactory.outPutClean("2017-10-18T17:18:47.928041", "10.81.28.192", "10.81.28.192", "farakav.com", "2017-10-18"),
                new DatasetFactory.outPutClean("2017-10-18T17:18:31.661994", "172.25.154.193", "172.25.154.193", "namnak.com", "2017-10-18"),
                new DatasetFactory.outPutClean("2017-10-18T17:16:45.226841", "100.92.218.151", "100.92.218.151", "salmonads.com", "2017-10-18"),
                new DatasetFactory.outPutClean("2017-10-18T17:17:34.028644", "100.82.150.116", "100.82.150.116", "filimo.com", "2017-10-18")
        );
        Dataset<DatasetFactory.outPutClean> expectedoutputclean = SparkHandlers.getSparkSession().createDataset(outputclean, DatasetFactory.getOutPutCleanEncoder());
        Dataset<Row> expectedDF = expectedoutputclean.toDF();
//        actualDF.printSchema();
//        assert contentOfDataFramesIsEqual(expectedDF, actualDF);
//        expectedDF.show();

        Dataset<Row> x = expectedDF.select(col("TimeStamp"), col("user_ip"),
                col("user_ID"), col("destination"), col("day"));
//        x.printSchema();
//               x.show();
//        x.printSchema();
        assert contentOfDataFramesIsEqual(x, actualDF);

    }

     
    
    /**
     * Test of getLogTypeID method, of class LogProcessing.
     */
}
