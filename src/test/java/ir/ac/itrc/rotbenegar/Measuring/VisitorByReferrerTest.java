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
import static org.junit.Assert.*;

public class VisitorByReferrerTest {

    private static VisitorByReferrer visitorByReferrer;
    private static Broadcast<Map<String, Integer>> refID;
    private static Integer logTypeID = 1000;

    //Referrer
    private static final String ref1 = "se";
    private static final String ref2 = "link";
    private static final String ref3 = "direct";

    //referrer IDs (NOTE: add appropriate assert commands in the setUpClass method)  
    private static Integer ref1ID;
    private static Integer ref2ID;
    private static Integer ref3ID;

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

    public VisitorByReferrerTest() {
    }

    private static Boolean contentOfDataFramesIsEqual(Dataset<Row> expectedDF, Dataset<Row> testDF) {
        Dataset<Row> df11 = expectedDF.select(col("logTypeID"), col("day"), col("domainID"), col("criterionID").as("referrerTypeID"), col("count"));
        Dataset<Row> df22 = testDF.select(col("logTypeID"), col("day"), col("domainID"), col("referrerTypeID"), col("count"));

        return df11.except(df22).count() == 0 && df22.except(df11).count() == 0;
    }

    @BeforeClass
    public static void setUpClass() {        
        DataFiles.setupMeasuringTests();
        
        refID = Criterion.loadID();
        ref1ID = refID.value().get(ref1);
        ref2ID = refID.value().get(ref2);
        ref3ID = refID.value().get(ref3);
        
        visitorByReferrer = new VisitorByReferrer();

        assert ref1ID != null;
        assert ref2ID != null;
        assert ref3ID != null;
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
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

        //compare results
        assertEquals(testResults, expectedResults);

    }

    @Test
    public void measuringFieldsFoundAllEmpty() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "safari", "winxp", ""),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "safari", "winxp", ""),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", "safari", "winxp", ""),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "safari", "winxp", "")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList();
        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0 && expectedResults.count() == 0;
    }

    @Test
    public void measuringFieldsFoundAllNull() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "safari", "winxp", null),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "safari", "winxp", null),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", "safari", "winxp", null),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "safari", "winxp", null)
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //run test
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0;

    }

    @Test
    public void measuringFieldsMixtureOf_Empty_Null_And_Data() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "safari", "winxp", ""),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "safari", "winxp", null),
                new DatasetFactory.DataFields(day2, varzesh3, 1554000043, "12347", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "safari", "winxp", null)
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), ref1ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }

    @Test
    public void measuringFieldsWithAllInvalidData() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "safari", "winxp", "()^&abc"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "safari", "winxp", "defsdlksdlsdlksdl"),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", "safari", "winxp", ""),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "safari", "winxp", "zyxjhhiu")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //run test
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

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
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0;
    }

    @Test
    public void nullInputDataframe() {
        //create data for test
        Dataset<Row> inputDF = null;

        //run test
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

        //compare results
        assertEquals(testResults, null);
    }

    @Test
    public void measuringFieldsMixtureOfCommonFieldsAndDataFields() {
        //create data for test
        List<DatasetFactory.CommonFields> data = Arrays.asList(
                new DatasetFactory.CommonFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "chrome", "winxp", ref1),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", "safari", "winxp", ref2),
                new DatasetFactory.CommonFields(1453939200, "itrc.ac.ir", 1454000044, "12346", "1.1.1.1"),
                new DatasetFactory.CommonFields(1453939200, "itrc.ac.ir", 1454000045, "12347", "1.1.1.1"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "zyxjhhiu", "winxp", "zyxjhhiu")
        );

        Dataset<DatasetFactory.CommonFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getCommonFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //run test
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

        //compare results
        assertEquals(testResults, null);
    }

    @Test
    public void measuringFieldsNotTrimmed() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "safari", "winxp", "      link     "),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "safari", "winxp", "          link"),
                new DatasetFactory.DataFields(1553939200, "varzesh3.com", 1554000043, "12347", "1.1.1.1", "safari", "winxp", "direct   "),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "safari",  "winxp", "    se         ")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //run test
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0;
    }

    @Test
    public void measuringFieldsNotLowered() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "Mozilla", "winxp", ref1.toUpperCase()),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12348", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12348", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day1, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "Chrome", "winxp", ref2.toUpperCase()),
                new DatasetFactory.DataFields(day2, "varzesh3.com", 1554000043, "12347", "1.1.1.1", "Chrome", "winxp", ref2.toUpperCase()),
                new DatasetFactory.DataFields(day2, varzesh3, 1554000043, "12348", "1.1.1.1", "safari", "winxp", ref2)
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), ref1ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), ref2ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);

    }

    @Test
    public void singleRecord() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref1)
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), ref1ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);

    }

    @Test
    public void measuringFieldsOneDayMultipleDomainsSingleRecordEach() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "Mozilla", "winxp", ref1.toUpperCase()),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "12348", "1.1.1.1", "safari", "winxp", ref3),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref2)
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), ref1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(persianblog), ref3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala), ref1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(mehrnews), ref2ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }

    @Test
    public void measuringFieldsOneDayMultipleDomains() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "Mozilla", "winxp", ref1.toUpperCase()),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref1),

                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref3),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref3),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "22346", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "32346", "1.1.1.1", "safari", "winxp", ref1),

                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "12348", "1.1.1.1", "safari", "winxp", null),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123480", "1.1.1.1", "safari", "winxp", ref2),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123481", "1.1.1.1", "safari", "winxp", ref2),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123482", "1.1.1.1", "safari", "winxp", ref2),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123483", "1.1.1.1", "safari", "winxp", ref2),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123483", "1.1.1.1", "safari", "winxp", ref2),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123483", "1.1.1.1", "safari", "winxp", ref2),

                new DatasetFactory.DataFields(day1, digikala, 1454000043, "123481", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "123482", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", "1.1.1.1", "safari", "winxp", ref2),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", "1.1.1.1", "safari", "winxp", ref2),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", "1.1.1.1", "safari", "winxp", ref3),

                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ""),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", "winxp", null),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref3),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref3)
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), ref1ID, 2),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), ref3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), ref1ID, 3),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(persianblog), ref2ID, 4),

                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala), ref1ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala), ref2ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala), ref3ID, 1),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(mehrnews), ref3ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }

    @Test
    public void measuringFieldsMultipleDaysSingleRecordEach() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "safari", "winxp", ref3),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "12348", "1.1.1.1", "safari", "winxp", ref3),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "12348", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day5, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref2)
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), ref3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), ref1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(persianblog), ref3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(digikala), ref1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(mehrnews), ref2ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);

    }

    @Test
    public void measuringFieldsMultipleDays() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "Mozilla", "winxp", ref1.toUpperCase()),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day2, itrc, 1454000043, "12345", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day2, itrc, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day3, itrc, 1454000043, "12345", "1.1.1.1", "Mozilla", "winxp", ref1.toUpperCase()),
                new DatasetFactory.DataFields(day5, itrc, 1454000043, "12345", "1.1.1.1", "safari", "winxp", ref2),
                new DatasetFactory.DataFields(day5, itrc, 1454000043, "12345", "1.1.1.1", "safari", "winxp", ref3),
                new DatasetFactory.DataFields(day5, itrc, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref3),

                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref3),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref3),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "22346", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "32346", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "22346", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "32346", "1.1.1.1", "safari", "winxp", ref1),

                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "12348", "1.1.1.1", "safari", "winxp", null),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "123480", "1.1.1.1", "safari", "winxp", ref2),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "123481", "1.1.1.1", "safari", "winxp", ref2),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "123482", "1.1.1.1", "safari", "winxp", ref2),
                new DatasetFactory.DataFields(day4, persianblog, 1454000043, "123483", "1.1.1.1", "safari", "winxp", ref3),
                new DatasetFactory.DataFields(day4, persianblog, 1454000043, "123483", "1.1.1.1", "safari", "winxp", ref3),
                new DatasetFactory.DataFields(day5, persianblog, 1454000043, "123483", "1.1.1.1", "safari", "winxp", ref1),

                new DatasetFactory.DataFields(day3, digikala, 1454000043, "123481", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day3, digikala, 1454000043, "123482", "1.1.1.1", "safari", "winxp", ref1),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "123480", "1.1.1.1", "safari", "winxp", ref2),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "12348", "1.1.1.1", "safari", "winxp", ref2),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "12348", "1.1.1.1", "safari", "winxp", ref3),

                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ""),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", "winxp", null),
                new DatasetFactory.DataFields(day2, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref3),
                new DatasetFactory.DataFields(day3, mehrnews, 1454000043, "12346", "1.1.1.1", "safari", "winxp", ref2)
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), ref1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(itrc), ref1ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(itrc), ref2ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(itrc), ref3ID, 2),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), ref3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), ref1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), ref3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), ref1ID, 3),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(persianblog), ref2ID, 3),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(persianblog), ref3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(persianblog), ref1ID, 1),

                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(digikala), ref1ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(digikala), ref2ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(digikala), ref3ID, 1),
                
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(mehrnews), ref3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(mehrnews), ref2ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByReferrer.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }
    
}
