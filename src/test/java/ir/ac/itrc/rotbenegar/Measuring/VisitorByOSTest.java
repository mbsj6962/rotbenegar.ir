/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.DataFormats.DatasetFactory;
import ir.ac.itrc.rotbenegar.Utilities.IDMaker;
import ir.ac.itrc.rotbenegar.Utilities.Logger;
import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import ir.ac.itrc.rotbenegar.Utilities.DataFiles;

import java.io.BufferedReader;
import java.io.IOException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.*;
import java.util.ArrayList;
import org.apache.spark.SparkConf;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Rule;

//import com.holdenkarau.spark.testing.SparkSessionProvider;
//import com.holdenkarau.spark.testing.SharedJavaSparkContext;
//import com.holdenkarau.spark.testing.TestSuite;
//import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
//import com.holdenkarau.spark.testing.DataFrameSuiteBase;
//import org.apache.spark.SparkContext;
//import org.apache.spark.rdd.RDD;
//import org.apache.spark.sql.SQLContext;

//import org.scalatest.FunSuite;
//import scala.Tuple2;
//import scala.reflect.ClassTag;

public class VisitorByOSTest {

    private static VisitorByOS visitorByOS;
    private static Broadcast<Map<String, Integer>> osID;
    private static Integer logTypeID = 1000;

    //OS
    private static final String os1 = "winxp";
    private static final String os2 = "win2008";
    private static final String os3 = "ubuntu";

    //OS IDs (NOTE: add appropriate assert commands in the setUpClass method)  
    private static Integer os1ID;
    private static Integer os2ID;
    private static Integer os3ID;

    //days
    private static final long day1 = 1453939200;
    private static final long day2 = 1553939200;
    private static final long day3 = 1653939200;
    private static final long day4 = 1753939200;
    private static final long day5 = 1853939200;

    //domains
    private static final String itrc = "itrc.ac.ir";
    private static final String varzesh3 = "varzesh3.com";
    private static final String persianblog = "persianblog.ir";
    private static final String mehrnews = "mehrnews.com";
    private static final String digikala = "digikala.com";

    
    public VisitorByOSTest() {
    }

    private static Boolean contentOfDataFramesIsEqual(Dataset<Row> expectedDF, Dataset<Row> testDF) {
        Dataset<Row> df11 = expectedDF.select(col("logTypeID"), col("day"), col("domainID"), col("criterionID").as("osID"), col("count"));
        Dataset<Row> df22 = testDF.select(col("logTypeID"), col("day"), col("domainID"), col("osID"), col("count"));

        return df11.except(df22).count() == 0 && df22.except(df11).count() == 0;
    }

//    private static void initilizeSpark() {
//        String appName = "test_measuring";
//        SparkConf conf = new SparkConf()
//                .setAppName(appName)
//                .setMaster("local[8]")
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC");
//            
//        spark = SparkSession
//                .builder()
//                .appName(appName)
//                .master("local[8]")
//                .config(conf)
//                .getOrCreate();        
//        
//    }


//    before
    
    
    @BeforeClass
    public static void setUpClass() {
        DataFiles.setupMeasuringTests();
        
        osID = Criterion.loadID();
        os1ID = osID.value().get(os1);
        os2ID = osID.value().get(os2);
        os3ID = osID.value().get(os3);        
        
        visitorByOS = new VisitorByOS();

        assert os1ID != null;
        assert os2ID != null;
        assert os3ID != null;
        
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

//    @Rule
//    public RunInThreadRule runInThread = new RunInThreadRule();
    
   @Test
    public void measuringFieldsNotFound() {
        //create data for test
        List<DatasetFactory.CommonFields> data = Arrays.asList(
                new DatasetFactory.CommonFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1"),
                new DatasetFactory.CommonFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1"),
                new DatasetFactory.CommonFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1"),
                new DatasetFactory.CommonFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1")
        );

        Dataset<DatasetFactory.CommonFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getCommonFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expected results
        Dataset<Row> expectedResults = null;

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assertEquals(testResults, expectedResults);

    }

   @Test
    public void measuringFieldsFoundAllEmpty() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "safari", "", "Link"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "safari", "", "Link"),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", "safari", "", "Link"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "safari", "", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList();
        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0 && expectedResults.count() == 0;
    }

   @Test
    public void measuringFieldsFoundAllNull() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "safari", null, "Link"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "safari", null, "Link"),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", "safari", null, "Link"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "safari", null, "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0;

    }

   @Test
    public void measuringFieldsMixtureOf_Empty_Null_And_Data() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "safari", "", "Link"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "safari", null, "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1554000043, "12347", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "safari", null, "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3),os1ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }

   @Test
    public void measuringFieldsWithAllInvalidData() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "safari", "()^&abc", "Link"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "safari", "defsdlksdlsdlksdl", "Link"),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", "safari", "", "Link"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "safari", "zyxjhhiu", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0;
    }

   @Test
    public void sizeZeroInputDataframe() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList();

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();
        assert inputDF.count() == 0;

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0;
    }

   @Test
    public void nullInputDataframe() {
        //create data for test
        Dataset<Row> inputDF = null;

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assertEquals(testResults, null);
    }

   @Test
    public void measuringFieldsMixtureOfCommonFieldsAndDataFields() {
        //create data for test
        List<DatasetFactory.CommonFields> data = Arrays.asList(
                new DatasetFactory.CommonFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "chrome", os1, "Link"),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", "safari", os2, "Link"),
                new DatasetFactory.CommonFields(1453939200, "itrc.ac.ir", 1454000044, "12346", "1.1.1.1"),
                new DatasetFactory.CommonFields(1453939200, "itrc.ac.ir", 1454000045, "12347", "1.1.1.1"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "zyxjhhiu", "zyxjhhiu", "Link")
        );

        Dataset<DatasetFactory.CommonFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getCommonFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assertEquals(testResults, null);
    }

   @Test
    public void measuringFieldsNotTrimmed() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "safari", "      winxp     ", "Link"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "safari", "          winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "varzesh3.com", 1554000043, "12347", "1.1.1.1", "safari", "winxp   ", "Link"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "safari",  "    winxp         ", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0;
    }

   @Test
    public void measuringFieldsNotLowered() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "Mozilla", os1.toUpperCase(), "Link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12346", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12348", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12348", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day1, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "Chrome", os2.toUpperCase(), "Link"),
                new DatasetFactory.DataFields(day2, "varzesh3.com", 1554000043, "12347", "1.1.1.1", "Chrome", os2.toUpperCase(), "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1554000043, "12348", "1.1.1.1", "safari", os2, "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc),os1ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), os2ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);

    }

   @Test
    public void singleRecord() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12346", "1.1.1.1", "safari", os1, "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc),os1ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);

    }

   @Test
    public void measuringFieldsOneDayMultipleDomainsSingleRecordEach() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "Mozilla", os1.toUpperCase(), "Link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "12348", "1.1.1.1", "safari", os3, "Link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", os2, "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3),os1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(persianblog), os3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala),os1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(mehrnews), os2ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }

   @Test
    public void measuringFieldsOneDayMultipleDomains() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "Mozilla", os1.toUpperCase(), "Link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12346", "1.1.1.1", "safari", os1, "Link"),

                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", os3, "Link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", os3, "Link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "22346", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "32346", "1.1.1.1", "safari", os1, "Link"),

                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "12348", "1.1.1.1", "safari", null, "Link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123480", "1.1.1.1", "safari", os2, "Link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123481", "1.1.1.1", "safari", os2, "Link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123482", "1.1.1.1", "safari",  os2, "Link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123483", "1.1.1.1", "safari", os2, "Link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123483", "1.1.1.1", "safari", os2, "Link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123483", "1.1.1.1", "safari", os2, "Link"),

                new DatasetFactory.DataFields(day1, digikala, 1454000043, "123481", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "123482", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", "1.1.1.1", "safari", os2, "Link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", "1.1.1.1", "safari", os2, "Link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", "1.1.1.1", "safari", os3, "Link"),

                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", "", "Link"),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", null, "Link"),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", os3, "Link"),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", os3, "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc),os1ID, 2),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), os3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3),os1ID, 3),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(persianblog), os2ID, 4),

                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala),os1ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala), os2ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala), os3ID, 1),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(mehrnews), os3ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }

   @Test
    public void measuringFieldsMultipleDaysSingleRecordEach() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "safari", os3, "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "12348", "1.1.1.1", "safari", os3, "Link"),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "12348", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day5, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", os2, "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), os3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3),os1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(persianblog), os3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(digikala),os1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(mehrnews), os2ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);

    }

   @Test
    public void measuringFieldsMultipleDays() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "Mozilla", os1.toUpperCase(), "Link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day2, itrc, 1454000043, "12345", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day2, itrc, 1454000043, "12346", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day3, itrc, 1454000043, "12345", "1.1.1.1", "Mozilla", os1.toUpperCase(), "Link"),
                new DatasetFactory.DataFields(day5, itrc, 1454000043, "12345", "1.1.1.1", "safari", os2, "Link"),
                new DatasetFactory.DataFields(day5, itrc, 1454000043, "12345", "1.1.1.1", "safari", os3, "Link"),
                new DatasetFactory.DataFields(day5, itrc, 1454000043, "12346", "1.1.1.1", "safari", os3, "Link"),

                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", os3, "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", os3, "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "22346", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "32346", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "22346", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "32346", "1.1.1.1", "safari", os1, "Link"),

                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "12348", "1.1.1.1", "safari", null, "Link"),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "123480", "1.1.1.1", "safari", os2, "Link"),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "123481", "1.1.1.1", "safari", os2, "Link"),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "123482", "1.1.1.1", "safari", os2, "Link"),
                new DatasetFactory.DataFields(day4, persianblog, 1454000043, "123483", "1.1.1.1", "safari", os3, "Link"),
                new DatasetFactory.DataFields(day4, persianblog, 1454000043, "123483", "1.1.1.1", "safari", os3, "Link"),
                new DatasetFactory.DataFields(day5, persianblog, 1454000043, "123483", "1.1.1.1", "safari", os1, "Link"),

                new DatasetFactory.DataFields(day3, digikala, 1454000043, "123481", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day3, digikala, 1454000043, "123482", "1.1.1.1", "safari", os1, "Link"),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "123480", "1.1.1.1", "safari", os2, "Link"),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "12348", "1.1.1.1", "safari", os2, "Link"),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "12348", "1.1.1.1", "safari", os3, "Link"),

                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", "", "Link"),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", null, "Link"),
                new DatasetFactory.DataFields(day2, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", os3, "Link"),
                new DatasetFactory.DataFields(day3, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", os2, "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc),os1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(itrc),os1ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(itrc), os2ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(itrc), os3ID, 2),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), os3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3),os1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), os3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3),os1ID, 3),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(persianblog), os2ID, 3),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(persianblog), os3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(persianblog),os1ID, 1),

                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(digikala),os1ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(digikala), os2ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(digikala), os3ID, 1),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(mehrnews), os3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(mehrnews), os2ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByOS.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }
}
