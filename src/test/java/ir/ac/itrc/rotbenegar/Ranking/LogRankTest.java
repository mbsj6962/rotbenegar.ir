/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Ranking;

import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author ammar
 */
public class LogRankTest {

    SparkHandlers spark = new SparkHandlers();
    String dir = System.getProperty("user.dir") + "/src/main/resources/datasink/";

    public LogRankTest() {
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
     * Test of computeScore method, of class LogRank. Multiple Days, Multiple
     * Domains, Multiple Users
     */
    @Test
    public void testComputeScoreMultipleDaysMultipleDomainsMultipleUsers() {
        System.out.println("computeScore");
        Dataset<Row> records = null;
        LogRank instance = new LogRank();

//        String dir = System.getProperty("user.dir") + "/src/main/resources/datasink/";
        Dataset<Row> expResult = SparkHandlers.getSparkSession().read().option("header", true).csv(dir + "MultipleDaysMultipleDomainsMultipleUsersResult.csv");
        Dataset<Row> result = SparkHandlers.getSparkSession().read().option("header", true).csv(dir + "MultipleDaysMultipleDomainsMultipleUsersSource.csv");
        result = instance.computeScore(prepairDataframe(result));
//        result.show(false);
        List<String> expResultLits = expResult.map(row -> row.mkString(), Encoders.STRING()).collectAsList();
        List<String> resultLits = result.map(row -> row.mkString(), Encoders.STRING()).collectAsList();
        assertEquals(expResultLits, resultLits);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of computeScore method, of class LogRank. One Day, Multiple Domains,
     * Multiple Users
     */
    @Test
    public void testComputeScoreOneDayMultipleDomainsMultipleUsers() {
        System.out.println("computeScore");
        Dataset<Row> records = null;
        LogRank instance = new LogRank();
//        String dir = System.getProperty("user.dir") + "/src/main/resources/datasink/";
        Dataset<Row> expResult = SparkHandlers.getSparkSession().read().option("header", true).csv(dir + "OneDayMultipleDomainsMultipleUsersResult.csv");
        Dataset<Row> result = SparkHandlers.getSparkSession().read().option("header", true).csv(dir + "OneDayMultipleDomainsMultipleUsersSource.csv");
        result = instance.computeScore(prepairDataframe(result));
//        result.show(false);
//        Dataset<Row> expResult = spark.getSparkSession().read().option("header", true).csv("/home/ammar/Downloads/results/OneDayMultipleDomainsMultipleUsersResult.csv");
//        Dataset<Row> result = instance.computeScore(prepairDataframe("OneDayMultipleDomainsMultipleUsersSource.csv"));
        List<String> expResultLits = expResult.map(row -> row.mkString(), Encoders.STRING()).collectAsList();
        List<String> resultLits = result.map(row -> row.mkString(), Encoders.STRING()).collectAsList();
        assertEquals(expResultLits, resultLits);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of computeScore method, of class LogRank. Multiple Days, One Domain,
     * Multiple Users
     */
    @Test
    public void testComputeScoreMultipleDaysOneDomainMultipleUsers() {
        System.out.println("computeScore");
        Dataset<Row> records = null;
        LogRank instance = new LogRank();
        Dataset<Row> expResult = SparkHandlers.getSparkSession().read().option("header", true).csv(dir + "MultipleDaysOneDomainMultipleUsersResult.csv");
        Dataset<Row> result = SparkHandlers.getSparkSession().read().option("header", true).csv(dir + "MultipleDaysOneDomainMultipleUsersSource.csv");
        result = instance.computeScore(prepairDataframe(result));
//        result.show(false);
//        Dataset<Row> expResult = spark.getSparkSession().read().option("header", true).csv("/home/ammar/Downloads/results/MultipleDaysOneDomainMultipleUsersResult.csv");
//        Dataset<Row> result = instance.computeScore(prepairDataframe("MultipleDaysOneDomainMultipleUsersSource.csv"));
        List<String> expResultLits = expResult.map(row -> row.mkString(), Encoders.STRING()).collectAsList();
        List<String> resultLits = result.map(row -> row.mkString(), Encoders.STRING()).collectAsList();
//        result.show(150);
//        result.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/ammar/Downloads/results/temp.csv");
        assertEquals(expResultLits, resultLits);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of computeScore method, of class LogRank. One Days, One Domain,
     * Multiple Users
     * New Test : Done
     */
    @Test
    public void testComputeScoreOneDayOneDomainMultipleUsers() {
        System.out.println("computeScore");
        Dataset<Row> records = null;
        LogRank instance = new LogRank();
        Dataset<Row> expResult = SparkHandlers.getSparkSession().read().option("header", true).csv(dir + "OneDaysOneDomainMultipleUsersResult.csv");
        Dataset<Row> result = SparkHandlers.getSparkSession().read().option("header", true).csv(dir + "OneDayOneDomainMultipleUsersSource.csv");
        result = instance.computeScore(prepairDataframe(result));
        List<String> expResultLits = expResult.map(row -> row.mkString(), Encoders.STRING()).collectAsList();
        List<String> resultLits = result.map(row -> row.mkString(), Encoders.STRING()).collectAsList();
//        result.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/ammar/Downloads/results/temp.csv");
        assertEquals(expResultLits, resultLits);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    public Dataset<Row> prepairDataframe(Dataset<Row> df) {
        //read csv file
//        Dataset<Row> df = spark.getSparkSession().read().option("header", true).csv("/home/ammar/Downloads/results/"+fileName);

        // change column dataType for casting
        df = df.withColumn("TimeStamp", df.col("TimeStamp").cast(IntegerType));
//        df = df.withColumn("destination", df.col("destination").cast(IntegerType));
//        df = df.withColumn("user_ID", df.col("user_ID").cast(IntegerType));
//        df = df.withColumn("user_ip", functions.lit(df.col("user_ip").cast(IntegerType)));
        df = df.withColumn("day", df.col("day").cast(DateType));
        return df;
    }

}