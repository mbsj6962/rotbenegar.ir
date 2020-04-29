/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.DataFormats.DatasetFactory;
import ir.ac.itrc.rotbenegar.Utilities.Configuration;
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

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import static org.junit.Assert.*;

import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Set;

public class VisitorByBrowserTest {

    private static VisitorByBrowser visitorByBrowser;
    private static Broadcast<Map<String, Integer>> browserID;
    private static Integer logTypeID = 1000;

    //browsers
    private static final String chrome = "chrome";
    private static final String safari = "safari";
    private static final String mozilla = "mozilla";

    //browser IDs (NOTE: add appropriate assert commands in the setUpClass method)  
    private static Integer chromeID;
    private static Integer safariID;
    private static Integer mozillaID;

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

    public VisitorByBrowserTest() {
    }

    private static Boolean contentOfDataFramesIsEqual(Dataset<Row> expectedDF, Dataset<Row> testDF) {
        Dataset<Row> df11 = expectedDF.select(col("logTypeID"), col("day"), col("domainID"), col("criterionID").as("browserID"), col("count"));
        Dataset<Row> df22 = testDF.select(col("logTypeID"), col("day"), col("domainID"), col("browserID"), col("count"));

        return df11.except(df22).count() == 0 && df22.except(df11).count() == 0;
    }

    @BeforeClass
    public static void setUpClass() {
        DataFiles.setupMeasuringTests();
        
        browserID = Criterion.loadID();
        chromeID = browserID.value().get(chrome);
        safariID = browserID.value().get(safari);
        mozillaID = browserID.value().get(mozilla);
    
        visitorByBrowser = new VisitorByBrowser();

        assert chromeID != null;
        assert safariID != null;
        assert mozillaID != null;
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
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);

        //compare results
        assertEquals(testResults, expectedResults);

    }

    @Test
    public void measuringFieldsFoundAllEmpty() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "", "winxp", "Link"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "", "winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", "", "winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "", "winxp", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList();
        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0 && expectedResults.count() == 0;
    }

    @Test
    public void measuringFieldsFoundAllNull() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", null, "winxp", "Link"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", null, "winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", null, "winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", null, "winxp", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //run test
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0;

    }

    @Test
    public void measuringFieldsMixtureOf_Empty_Null_And_Data() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "", "winxp", "Link"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", null, "winxp", "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1554000043, "12347", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", null, "winxp", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), chromeID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();
//expectedResults.write().format("com.databricks.spark.csv").option("header", "true").save("expectedResults");

        //run test
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);
//testResults.write().format("com.databricks.spark.csv").option("header", "true").save("testResults");

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }

    @Test
    public void measuringFieldsWithAllInvalidData() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "()^&abc", "winxp", "Link"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "defsdlksdlsdlksdl", "winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", "", "winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "zyxjhhiu", "winxp", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //run test
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);

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
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0;
    }

    @Test
    public void nullInputDataframe() {
        //create data for test
        Dataset<Row> inputDF = null;

        //run test
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);

        //compare results
        assertEquals(testResults, null);
    }

    @Test
    public void measuringFieldsMixtureOfCommonFieldsAndDataFields() {
        //create data for test
        List<DatasetFactory.CommonFields> data = Arrays.asList(
                new DatasetFactory.CommonFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "chrome", "winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", "safari", "winxp", "Link"),
                new DatasetFactory.CommonFields(1453939200, "itrc.ac.ir", 1454000044, "12346", "1.1.1.1"),
                new DatasetFactory.CommonFields(1453939200, "itrc.ac.ir", 1454000045, "12347", "1.1.1.1"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "zyxjhhiu", "winxp", "Link")
        );

        Dataset<DatasetFactory.CommonFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getCommonFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //run test
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);

        //compare results
        assertEquals(testResults, null);
    }

    @Test
    public void measuringFieldsNotTrimmed() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "       mozilla     ", "winxp", "Link"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "           chrome", "winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "varzesh3.com", 1554000043, "12347", "1.1.1.1", " chrome   ", "winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "safari         ", "winxp", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //run test
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0;
    }

    @Test
    public void measuringFieldsNotLowered() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", mozilla.toUpperCase(), "winxp", "Link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12346", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12348", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12348", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", chrome.toUpperCase(), "winxp", "Link"),
                new DatasetFactory.DataFields(day2, "varzesh3.com", 1554000043, "12347", "1.1.1.1", chrome.toUpperCase(), "winxp", "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1554000043, "12348", "1.1.1.1", safari, "winxp", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), chromeID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), safariID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);

    }

    @Test
    public void singleRecord() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12346", "1.1.1.1", chrome, "winxp", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), chromeID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);

    }

    @Test
    public void measuringFieldsOneDayMultipleDomainsSingleRecordEach() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "Mozilla", "winxp", "Link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "12348", "1.1.1.1", mozilla, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", safari, "winxp", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), chromeID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(persianblog), mozillaID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala), chromeID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(mehrnews), safariID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }

    @Test
    public void measuringFieldsOneDayMultipleDomains() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "Mozilla", "winxp", "Link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12346", "1.1.1.1", chrome, "winxp", "Link"),

                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", mozilla, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", mozilla, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "22346", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "32346", "1.1.1.1", chrome, "winxp", "Link"),

                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "12348", "1.1.1.1", null, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123480", "1.1.1.1", safari, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123481", "1.1.1.1", safari, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123482", "1.1.1.1", safari, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123483", "1.1.1.1", safari, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123483", "1.1.1.1", safari, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123483", "1.1.1.1", safari, "winxp", "Link"),

                new DatasetFactory.DataFields(day1, digikala, 1454000043, "123481", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "123482", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", "1.1.1.1", safari, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", "1.1.1.1", safari, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", "1.1.1.1", mozilla, "winxp", "Link"),

                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "", "winxp", "Link"),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", null, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", mozilla, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", mozilla, "winxp", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), chromeID, 2),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), mozillaID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), chromeID, 3),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(persianblog), safariID, 4),

                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala), chromeID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala), safariID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala), mozillaID, 1),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(mehrnews), mozillaID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }

    @Test
    public void measuringFieldsMultipleDaysSingleRecordEach() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", mozilla, "winxp", "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "12346", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "12348", "1.1.1.1", mozilla, "winxp", "Link"),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "12348", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day5, mehrnews, 1454000043, "12346", "1.1.1.1", safari, "winxp", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), mozillaID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), chromeID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(persianblog), mozillaID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(digikala), chromeID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(mehrnews), safariID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);

    }

    @Test
    public void measuringFieldsMultipleDays() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "Mozilla", "winxp", "Link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day2, itrc, 1454000043, "12345", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day2, itrc, 1454000043, "12346", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day3, itrc, 1454000043, "12345", "1.1.1.1", "Mozilla", "winxp", "Link"),
                new DatasetFactory.DataFields(day5, itrc, 1454000043, "12345", "1.1.1.1", safari, "winxp", "Link"),
                new DatasetFactory.DataFields(day5, itrc, 1454000043, "12345", "1.1.1.1", mozilla, "winxp", "Link"),
                new DatasetFactory.DataFields(day5, itrc, 1454000043, "12346", "1.1.1.1", mozilla, "winxp", "Link"),

                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", mozilla, "winxp", "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "12346", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "12346", "1.1.1.1", mozilla, "winxp", "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "22346", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "32346", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "22346", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "32346", "1.1.1.1", chrome, "winxp", "Link"),

                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "12348", "1.1.1.1", null, "winxp", "Link"),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "123480", "1.1.1.1", safari, "winxp", "Link"),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "123481", "1.1.1.1", safari, "winxp", "Link"),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "123482", "1.1.1.1", safari, "winxp", "Link"),
                new DatasetFactory.DataFields(day4, persianblog, 1454000043, "123483", "1.1.1.1", mozilla, "winxp", "Link"),
                new DatasetFactory.DataFields(day4, persianblog, 1454000043, "123483", "1.1.1.1", mozilla, "winxp", "Link"),
                new DatasetFactory.DataFields(day5, persianblog, 1454000043, "123483", "1.1.1.1", chrome, "winxp", "Link"),

                new DatasetFactory.DataFields(day3, digikala, 1454000043, "123481", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day3, digikala, 1454000043, "123482", "1.1.1.1", chrome, "winxp", "Link"),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "123480", "1.1.1.1", safari, "winxp", "Link"),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "12348", "1.1.1.1", safari, "winxp", "Link"),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "12348", "1.1.1.1", mozilla, "winxp", "Link"),

                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "", "winxp", "Link"),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", null, "winxp", "Link"),
                new DatasetFactory.DataFields(day2, mehrnews, 1454000043, "12346", "1.1.1.1", mozilla, "winxp", "Link"),
                new DatasetFactory.DataFields(day3, mehrnews, 1454000043, "12346", "1.1.1.1", safari, "winxp", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), chromeID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(itrc), chromeID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(itrc), safariID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(itrc), mozillaID, 2),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), mozillaID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), chromeID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), mozillaID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), chromeID, 3),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(persianblog), safariID, 3),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(persianblog), mozillaID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(persianblog), chromeID, 1),

                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(digikala), chromeID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(digikala), safariID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(digikala), mozillaID, 1),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(mehrnews), mozillaID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(mehrnews), safariID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByBrowser.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }
}
