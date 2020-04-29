/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Measuring;

import com.maxmind.geoip.LookupService;
import ir.ac.itrc.rotbenegar.DataFormats.DatasetFactory;
import ir.ac.itrc.rotbenegar.Utilities.IDMaker;
import ir.ac.itrc.rotbenegar.Utilities.Logger;
import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import static ir.ac.itrc.rotbenegar.Utilities.DataFiles.getVisitorByProvincePath;

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

public class VisitorByCountryTest {

    private static VisitorByCountry visitorByCountry;
//    private static LookupService lookupService = GeoServices.getGeoData();
    private static Broadcast<Map<String, Integer>> countryID;
    private static Integer logTypeID = 1000;

    //Referrer
    private static final String userIP1 = "188.158.130.221";//IR
    private static final String country1 = "ir";
    
    private static final String userIP2 = "37.48.64.200";//NL
    private static final String country2 = "nl";
    
    private static final String userIP3 = "206.190.36.45";//USA
    private static final String country3 = "us";

    //referrer IDs (NOTE: add appropriate assert commands in the setUpClass method)  
    private static Integer userIP1ID;
    private static Integer userIP2ID;
    private static Integer userIP3ID;

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

    public VisitorByCountryTest() {
    }

    private static Boolean contentOfDataFramesIsEqual(Dataset<Row> expectedDF, Dataset<Row> testDF) {
        Dataset<Row> df11 = expectedDF.select(col("logTypeID"), col("day"), col("domainID"), col("criterionID").as(Country.getCountryIDColName()), col("count"));
        Dataset<Row> df22 = testDF.select(col("logTypeID"), col("day"), col("domainID"), col(Country.getCountryIDColName()), col("count"));

        return df11.except(df22).count() == 0 && df22.except(df11).count() == 0;
    }
    
//    private static String getCountryName(String userIP) {
//        return lookupService.getLocation(userIP).countryName;
//    }

    @BeforeClass
    public static void setUpClass() {
        DataFiles.setupMeasuringTests();

        countryID = Criterion.loadID();
        userIP1ID = countryID.value().get(country1);
        userIP2ID = countryID.value().get(country2);
        userIP3ID = countryID.value().get(country3);
        
        visitorByCountry = new VisitorByCountry();

        assert userIP1ID != null;
        assert userIP2ID != null;
        assert userIP3ID != null;
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
        List<DataMissingUserip> data = Arrays.asList(
                new DataMissingUserip(1453939200, "itrc.ac.ir", 1454000043, "12345"),
                new DataMissingUserip(1453939200, "itrc.ac.ir", 1454000043, "12346"),
                new DataMissingUserip(1553939200, "google.com", 1554000043, "12347"),
                new DataMissingUserip(1553939200, "itrc.ac.ir", 1554000043, "12348")
        );

        Dataset<DataMissingUserip> records = SparkHandlers.getSparkSession().createDataset(data, getDataMissingUseripEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expected results
        Dataset<Row> expectedResults = null;

        //run test
        Dataset<Row> testResults = visitorByCountry.persistAndReturn(inputDF, logTypeID);

        //compare results
        assertEquals(testResults, expectedResults);

    }

   @Test
    public void measuringFieldsFoundAllEmpty() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "", "safari", "winxp", ""),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "", "safari", "winxp", ""),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "", "safari", "winxp", ""),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "", "safari", "winxp", "")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList();
        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByCountry.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0 && expectedResults.count() == 0;
    }

    @Test
    public void measuringFieldsFoundAllNull() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", null, "safari", "winxp", null),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", null, "safari", "winxp", null),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", null, "safari", "winxp", null),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", null, "safari", "winxp", null)
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //run test
        Dataset<Row> testResults = visitorByCountry.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0;

    }

    @Test
    public void measuringFieldsMixtureOf_Empty_Null_And_Data() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "", "safari", "winxp", ""),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", null, "safari", "winxp", null),
                new DatasetFactory.DataFields(day2, varzesh3, 1554000043, "12347", userIP1, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", null, "safari", "winxp", null)
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), userIP1ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();
        
        //run test
        Dataset<Row> testResults = visitorByCountry.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }

    @Test
    public void measuringFieldsWithAllInvalidData() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "15354.1.1.1", "safari", "winxp", "()^&abc"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.dsffdg.1.1", "safari", "winxp", "defsdlksdlsdlksdl"),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "dtyhy tyt", "safari", "winxp", ""),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1000.1231.1.121", "safari", "winxp", "zyxjhhiu")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //run test
        Dataset<Row> testResults = visitorByCountry.persistAndReturn(inputDF, logTypeID);

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
        Dataset<Row> testResults = visitorByCountry.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert testResults.count() == 0;
    }

    @Test
    public void nullInputDataframe() {
        //create data for test
        Dataset<Row> inputDF = null;

        //run test
        Dataset<Row> testResults = visitorByCountry.persistAndReturn(inputDF, logTypeID);

        //compare results
        assertEquals(testResults, null);
    }

    @Test
    public void measuringFieldsMixtureOfCommonFieldsAndDataFields() {
        //create data for test
        List<DatasetFactory.CommonFields> data = Arrays.asList(
                new DatasetFactory.CommonFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1000.1.1.1"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12346", userIP1, "chrome", "winxp", "link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1554000043, "12347", userIP2, "safari", "winxp", "link"),
                new DatasetFactory.CommonFields(1453939200, "itrc.ac.ir", 1454000044, "12346", "19991"),
                new DatasetFactory.CommonFields(1453939200, "itrc.ac.ir", 1454000045, "12347", "456.1"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "zyxjhhiu", "zyxjhhiu", "winxp", "zyxjhhiu")
        );

        Dataset<DatasetFactory.CommonFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getCommonFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), userIP1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), userIP2ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByCountry.persistAndReturn(inputDF, logTypeID);

        //compare results
        contentOfDataFramesIsEqual(expectedResults, testResults);
    }

    @Test
    public void measuringFieldsNotTrimmed() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "    "+userIP1, "safari", "winxp", "      link     "),
                new DatasetFactory.DataFields(day2, itrc, 1454000043, "12346", userIP1+"\t\t\t", "safari", "winxp", "          link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1554000043, "12347", "   \t"+userIP1+"    ", "safari", "winxp", "direct   "),
                new DatasetFactory.DataFields(day2, persianblog, 1554000043, "12348", "   "+userIP3+"     ", "safari", "winxp", "    se         ")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), userIP1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(itrc), userIP1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), userIP1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(persianblog), userIP3ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByCountry.persistAndReturn(inputDF, logTypeID);

        //compare results
        contentOfDataFramesIsEqual(expectedResults, testResults);
    }

    @Test
    public void singleRecord() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12346", userIP1, "safari", "winxp", "link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), userIP1ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByCountry.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);

    }

    @Test
    public void measuringFieldsOneDayMultipleDomainsSingleRecordEach() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", " sd ksds owe", "Mozilla", "winxp", "link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", userIP1, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "12348", userIP3, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", userIP1, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", userIP2, "safari", "winxp", "link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), userIP1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(persianblog), userIP3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala), userIP1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(mehrnews), userIP2ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByCountry.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }

    @Test
    public void measuringFieldsOneDayMultipleDomains() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "1000.1000.1.1", "Mozilla", "winxp", "link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", userIP1, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", userIP1, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12346", userIP1, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", userIP3, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", userIP1, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", userIP3, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "22346", userIP1, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "32346", userIP1, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "12348", null, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123480", userIP2, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123481", userIP2, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123482", userIP2, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123483", userIP2, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123483", userIP2, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, persianblog, 1454000043, "123483", userIP2, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "123481", userIP1, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "123482", userIP1, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", userIP2, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", userIP2, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, digikala, 1454000043, "12348", userIP3, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "", "safari", "winxp", ""),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", null, "safari", "winxp", null),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", userIP3, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", userIP3, "safari", "winxp", "link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), userIP1ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), userIP3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), userIP1ID, 3),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(persianblog), userIP2ID, 4),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala), userIP1ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala), userIP2ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(digikala), userIP3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(mehrnews), userIP3ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByCountry.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }

    @Test
    public void measuringFieldsMultipleDaysSingleRecordEach() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", userIP3, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "12346", userIP1, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "12348", userIP3, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "12348", userIP1, "safari", "winxp", "link"),
                new DatasetFactory.DataFields(day5, mehrnews, 1454000043, "12346", userIP2, "safari", "winxp", "link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), userIP3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), userIP1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(persianblog), userIP3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(digikala), userIP1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(mehrnews), userIP2ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByCountry.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);

    }

    @Test
    public void measuringFieldsMultipleDays() {
        //create data for test
        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", "sdak sa sdlas  ", "Mozilla", "winxp", userIP1.toUpperCase()),
                new DatasetFactory.DataFields(day1, itrc, 1454000043, "12345", userIP1, "safari", "winxp", userIP1),
                new DatasetFactory.DataFields(day2, itrc, 1454000043, "12345", userIP1, "safari", "winxp", userIP1),
                new DatasetFactory.DataFields(day2, itrc, 1454000043, "12346", userIP1, "safari", "winxp", userIP1),
                new DatasetFactory.DataFields(day3, itrc, 1454000043, "12345", "     ", "Mozilla", "winxp", userIP1.toUpperCase()),
                new DatasetFactory.DataFields(day5, itrc, 1454000043, "12345", userIP2, "safari", "winxp", userIP2),
                new DatasetFactory.DataFields(day5, itrc, 1454000043, "12345", userIP3, "safari", "winxp", userIP3),
                new DatasetFactory.DataFields(day5, itrc, 1454000043, "12346", userIP3, "safari", "winxp", userIP3),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "12346", userIP3, "safari", "winxp", userIP3),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "12346", userIP1, "safari", "winxp", userIP1),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "12346", userIP3, "safari", "winxp", userIP3),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "22346", userIP1, "safari", "winxp", userIP1),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "32346", userIP1, "safari", "winxp", userIP1),
                new DatasetFactory.DataFields(day2, varzesh3, 1454000043, "22346", userIP1, "safari", "winxp", userIP1),
                new DatasetFactory.DataFields(day1, varzesh3, 1454000043, "32346", userIP1, "safari", "winxp", userIP1),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "12348", null, "safari", "winxp", null),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "123480", userIP2, "safari", "winxp", userIP2),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "123481", userIP2, "safari", "winxp", userIP2),
                new DatasetFactory.DataFields(day3, persianblog, 1454000043, "123482", userIP2, "safari", "winxp", userIP2),
                new DatasetFactory.DataFields(day4, persianblog, 1454000043, "123483", userIP3, "safari", "winxp", userIP3),
                new DatasetFactory.DataFields(day4, persianblog, 1454000043, "123483", userIP3, "safari", "winxp", userIP3),
                new DatasetFactory.DataFields(day5, persianblog, 1454000043, "123483", userIP1, "safari", "winxp", userIP1),
                new DatasetFactory.DataFields(day3, digikala, 1454000043, "123481", userIP1, "safari", "winxp", userIP1),
                new DatasetFactory.DataFields(day3, digikala, 1454000043, "123482", userIP1, "safari", "winxp", userIP1),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "123480", userIP2, "safari", "winxp", userIP2),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "12348", userIP2, "safari", "winxp", userIP2),
                new DatasetFactory.DataFields(day4, digikala, 1454000043, "12348", userIP3, "safari", "winxp", userIP3),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", "", "safari", "winxp", ""),
                new DatasetFactory.DataFields(day1, mehrnews, 1454000043, "12346", null, "safari", "winxp", null),
                new DatasetFactory.DataFields(day2, mehrnews, 1454000043, "12346", userIP3, "safari", "winxp", userIP3),
                new DatasetFactory.DataFields(day3, mehrnews, 1454000043, "12346", userIP2, "safari", "winxp", userIP2)
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> inputDF = records.toDF();

        //expeted results
        List<DatasetFactory.ResultingFieldsForMeasuring> expectedDataList = Arrays.asList(
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(itrc), userIP1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(itrc), userIP1ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(itrc), userIP2ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(itrc), userIP3ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), userIP3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day1, IDMaker.getTargetSiteID(varzesh3), userIP1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), userIP3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(varzesh3), userIP1ID, 3),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(persianblog), userIP2ID, 3),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(persianblog), userIP3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day5, IDMaker.getTargetSiteID(persianblog), userIP1ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(digikala), userIP1ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(digikala), userIP2ID, 2),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day4, IDMaker.getTargetSiteID(digikala), userIP3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day2, IDMaker.getTargetSiteID(mehrnews), userIP3ID, 1),
                new DatasetFactory.ResultingFieldsForMeasuring(logTypeID, day3, IDMaker.getTargetSiteID(mehrnews), userIP2ID, 1)
        );

        Dataset<DatasetFactory.ResultingFieldsForMeasuring> expectedResultsDataFields = SparkHandlers.getSparkSession().createDataset(expectedDataList, DatasetFactory.getResultingFieldsForMeasuringEncoder());
        Dataset<Row> expectedResults = expectedResultsDataFields.toDF();

        //run test
        Dataset<Row> testResults = visitorByCountry.persistAndReturn(inputDF, logTypeID);

        //compare results
        assert contentOfDataFramesIsEqual(expectedResults, testResults);
    }

    
    
    private static Encoder<DataMissingUserip> dataMissingUseripEncoder = Encoders.bean(DataMissingUserip.class);
    
    public static Encoder<DataMissingUserip> getDataMissingUseripEncoder() {
        return dataMissingUseripEncoder;
    }
     
    public static class DataMissingUserip {

        protected long day;
        protected String domain;
        protected long timestamp;
        protected String userID;

        public DataMissingUserip(long day, String domain, long timestamp, String userID) {
            this.day = day;
            this.domain = domain;
            this.timestamp = timestamp;
            this.userID = userID;
        }

        public long getDay() {
            return day;
        }

        public void setDay(long day) {
            this.day = day;
        }

        public String getDomain() {
            return domain;
        }

        public void setDomain(String domain) {
            this.domain = domain;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public String getUserID() {
            return userID;
        }

        public void setUserID(String userID) {
            this.userID = userID;
        }
    }

}
