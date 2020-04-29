/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Pipeline;

import ir.ac.itrc.rotbenegar.Pipeline.RanksAndMeasures;

import ir.ac.itrc.rotbenegar.DataFormats.DatasetFactory;
import ir.ac.itrc.rotbenegar.Ranking.LogRank;
import ir.ac.itrc.rotbenegar.Ranking.Score;
import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import ir.ac.itrc.rotbenegar.Utilities.IDMaker;
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
public class RanksAndMeasuresTest {
//    private static Score score;

    private static Boolean contentOfDataFramesIsEqual(Dataset<Row> expectedDF, Dataset<Row> testDF) {
        Dataset<Row> df11 = expectedDF.select(col("dailySessionCount"), col("dailySessionDuration"),
                col("dailyVisitCount"), col("dailyVisitorCount"), col("day"),
                col("domain"), col("rankScore"), col("targetSiteID"), col("logTypeID"), col("rankNum"));
//df11.write().format("com.databricks.spark.csv").option("header", "true").save("expectedOutput");        

        Dataset<Row> df22 = testDF.select(col("dailySessionCount"), col("dailySessionDuration"),
                col("dailyVisitCount"), col("dailyVisitorCount"), col("day"),
                col("domain"), col("rankScore"), col("targetSiteID"), col("logTypeID"), col("rankNum"));
//df22.write().format("com.databricks.spark.csv").option("header", "true").save("actualOutPut");

        return df11.except(df22).count() == 0 && df22.except(df11).count() == 0;
    }

    public RanksAndMeasuresTest() {
    }

    @BeforeClass
    public static void setUpClass() {
//        score = new LogRank();
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
    public void testPersistRank() {

        List<DatasetFactory.targetSiteTest> actualDataDaily = Arrays.asList(
                new DatasetFactory.targetSiteTest(1454000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 1.0), //DataSet For Test PersistRank
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0)
        );

        Dataset<DatasetFactory.targetSiteTest> actualDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(actualDataDaily, DatasetFactory.getTargetSiteTestEncoder());
        Dataset<Row> actualDataDailyRecordsDF = actualDataDailyRecords.toDF();

        String[] domains = {"itrc.ac.ir", "google.com"};

        List<DatasetFactory.DFWithAdditionalColumnTest> expectedOutPut;
        expectedOutPut = Arrays.asList(
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, domains[0], 4.0, IDMaker.getTargetSiteID(domains[0]), 1, 1),
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, domains[1], 3.0, IDMaker.getTargetSiteID(domains[1]), 1, 2),
                new DatasetFactory.DFWithAdditionalColumnTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, domains[0], 2.0, IDMaker.getTargetSiteID(domains[0]), 1, 3),
                new DatasetFactory.DFWithAdditionalColumnTest(1454000043, 1454000043, 1454000043, 1454000043, 1454000043, domains[0], 1.0, IDMaker.getTargetSiteID(domains[0]), 1, 4)
        );

        Dataset<DatasetFactory.DFWithAdditionalColumnTest> expectedOutPutRecords
                = SparkHandlers.getSparkSession().createDataset(expectedOutPut, DatasetFactory.getDFWithAdditionalColumnTestEncoder());
        Dataset<Row> expectedDFWithAdditionalRecordsDF = expectedOutPutRecords.toDF();

        RanksAndMeasures instance2 = new RanksAndMeasures();

        Dataset<Row> actualDF = instance2.persistRank2(actualDataDailyRecordsDF, 1);

//        DataFiles.saveTargetSitePath(actualDF);
//        DataFiles.saveDailyVisitRankPath(expectedDFWithAdditionalRecordsDF);
//        actualDF.printSchema();
//        expectedDFWithAdditionalRecordsDF.printSchema();
        assert contentOfDataFramesIsEqual(actualDF, expectedDFWithAdditionalRecordsDF);
    }
// ------------         ----------    ---------------------------------          ------------------------

    @Test
    public void IncorcetValueOfDailySessionCount() {

        List<DatasetFactory.targetSiteTest> actualDataDaily = Arrays.asList(
                new DatasetFactory.targetSiteTest(-1, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 1.0), //DataSet For Test PersistRank
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0)
        );

        Dataset<DatasetFactory.targetSiteTest> actualDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(actualDataDaily, DatasetFactory.getTargetSiteTestEncoder());
        Dataset<Row> actualDataDailyRecordsDF = actualDataDailyRecords.toDF();

        String[] domains = {"itrc.ac.ir", "google.com"};

        List<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDaily = Arrays.asList(
                //DataSet For Test PersistRank
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0, IDMaker.getTargetSiteID(domains[0]), 1, 1),
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0, IDMaker.getTargetSiteID(domains[1]), 1, 2),
                new DatasetFactory.DFWithAdditionalColumnTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0, IDMaker.getTargetSiteID(domains[0]), 1, 3)
        );

        Dataset<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(expectedDataDaily, DatasetFactory.getDFWithAdditionalColumnTestEncoder());
        Dataset<Row> expectedDataDailyRecordsDF = expectedDataDailyRecords.toDF();

        RanksAndMeasures instance2 = new RanksAndMeasures();

        Dataset<Row> actualDF = instance2.persistRank2(actualDataDailyRecordsDF, 1);

        Dataset<Row> actualDF1 = expectedDataDailyRecordsDF.select(col("dailySessionCount"),
                col("dailySessionDuration"), col("dailyVisitCount"),
                col("dailyVisitorCount"), col("day"), col("domain"), col("rankScore"),
                col("targetSiteID"), col("logTypeID"), col("rankNum"));
//        actualDF.show();
//        actualDF1.show();

//        DataFiles.saveDailyVisitRankPath(actualDF);
//        DataFiles.saveTargetSitePath(actualDF1);
        assert contentOfDataFramesIsEqual(actualDF1, actualDF);
    }

    @Test
    public void incorrectValueOfDailySessionDuration() {

        List<DatasetFactory.targetSiteTest> actualDataDaily = Arrays.asList(
                new DatasetFactory.targetSiteTest(1554000043, -1, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 1.0), //DataSet For Test PersistRank
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0)
        );

        Dataset<DatasetFactory.targetSiteTest> actualDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(actualDataDaily, DatasetFactory.getTargetSiteTestEncoder());
        Dataset<Row> actualDataDailyRecordsDF = actualDataDailyRecords.toDF();

        String[] domains = {"itrc.ac.ir", "google.com"};

        List<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDaily = Arrays.asList(
                //DataSet For Test PersistRank
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0, IDMaker.getTargetSiteID(domains[0]), 1, 1),
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0, IDMaker.getTargetSiteID(domains[1]), 1, 2),
                new DatasetFactory.DFWithAdditionalColumnTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0, IDMaker.getTargetSiteID(domains[0]), 1, 3)
        );

        Dataset<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(expectedDataDaily, DatasetFactory.getDFWithAdditionalColumnTestEncoder());
        Dataset<Row> expectedDataDailyRecordsDF = expectedDataDailyRecords.toDF();

        RanksAndMeasures instance2 = new RanksAndMeasures();

        Dataset<Row> actualDF = instance2.persistRank2(actualDataDailyRecordsDF, 1);

        Dataset<Row> actualDF1 = expectedDataDailyRecordsDF.select(col("dailySessionCount"),
                col("dailySessionDuration"), col("dailyVisitCount"),
                col("dailyVisitorCount"), col("day"), col("domain"), col("rankScore"),
                col("targetSiteID"), col("logTypeID"), col("rankNum"));
//        actualDF.show();
//        actualDF1.show();
//
//        DataFiles.saveDailyVisitRankPath(actualDF);
//        DataFiles.saveTargetSitePath(actualDF1);
        assert contentOfDataFramesIsEqual(actualDF1, actualDF);

    }

    @Test
    public void incorrectValueOfDailyVisitCount() {

        List<DatasetFactory.targetSiteTest> actualDataDaily = Arrays.asList(
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, -1, 1454000043, 1454000043, "itrc.ac.ir", 1.0), //DataSet For Test PersistRank
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0)
        );

        Dataset<DatasetFactory.targetSiteTest> actualDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(actualDataDaily, DatasetFactory.getTargetSiteTestEncoder());
        Dataset<Row> actualDataDailyRecordsDF = actualDataDailyRecords.toDF();

        String[] domains = {"itrc.ac.ir", "google.com"};

        List<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDaily = Arrays.asList(
                //DataSet For Test PersistRank
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0, IDMaker.getTargetSiteID(domains[0]), 1, 1),
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0, IDMaker.getTargetSiteID(domains[1]), 1, 2),
                new DatasetFactory.DFWithAdditionalColumnTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0, IDMaker.getTargetSiteID(domains[0]), 1, 3)
        );

        Dataset<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(expectedDataDaily, DatasetFactory.getDFWithAdditionalColumnTestEncoder());
        Dataset<Row> expectedDataDailyRecordsDF = expectedDataDailyRecords.toDF();

        RanksAndMeasures instance2 = new RanksAndMeasures();

        Dataset<Row> actualDF = instance2.persistRank2(actualDataDailyRecordsDF, 1);

        Dataset<Row> actualDF1 = expectedDataDailyRecordsDF.select(col("dailySessionCount"),
                col("dailySessionDuration"), col("dailyVisitCount"),
                col("dailyVisitorCount"), col("day"), col("domain"), col("rankScore"),
                col("targetSiteID"), col("logTypeID"), col("rankNum"));
//        actualDF.show();
//        actualDF1.show();

//        DataFiles.saveDailyVisitRankPath(actualDF);
//        DataFiles.saveTargetSitePath(actualDF1);
        assert contentOfDataFramesIsEqual(actualDF1, actualDF);

    }

    @Test
    public void incorrectValueOfDailyVisitorCount() {

        List<DatasetFactory.targetSiteTest> actualDataDaily = Arrays.asList(
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, -1, 1454000043, "itrc.ac.ir", 1.0), //DataSet For Test PersistRank
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0)
        );

        Dataset<DatasetFactory.targetSiteTest> actualDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(actualDataDaily, DatasetFactory.getTargetSiteTestEncoder());
        Dataset<Row> actualDataDailyRecordsDF = actualDataDailyRecords.toDF();

        String[] domains = {"itrc.ac.ir", "google.com"};

        List<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDaily = Arrays.asList(
                //DataSet For Test PersistRank
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0, IDMaker.getTargetSiteID(domains[0]), 1, 1),
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0, IDMaker.getTargetSiteID(domains[1]), 1, 2),
                new DatasetFactory.DFWithAdditionalColumnTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0, IDMaker.getTargetSiteID(domains[0]), 1, 3)
        );

        Dataset<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(expectedDataDaily, DatasetFactory.getDFWithAdditionalColumnTestEncoder());
        Dataset<Row> expectedDataDailyRecordsDF = expectedDataDailyRecords.toDF();

        RanksAndMeasures instance2 = new RanksAndMeasures();

        Dataset<Row> actualDF = instance2.persistRank2(actualDataDailyRecordsDF, 1);

        Dataset<Row> actualDF1 = expectedDataDailyRecordsDF.select(col("dailySessionCount"),
                col("dailySessionDuration"), col("dailyVisitCount"),
                col("dailyVisitorCount"), col("day"), col("domain"), col("rankScore"),
                col("targetSiteID"), col("logTypeID"), col("rankNum"));
//        actualDF.show();
//        actualDF1.show();

//        DataFiles.saveDailyVisitRankPath(actualDF);
//        DataFiles.saveTargetSitePath(actualDF1);
        assert contentOfDataFramesIsEqual(actualDF1, actualDF);

    }

    @Test
    public void incorrectValueOfRankScore() {

        List<DatasetFactory.targetSiteTest> actualDataDaily = Arrays.asList(
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", -1.0), //DataSet For Test PersistRank
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0)
        );

        Dataset<DatasetFactory.targetSiteTest> actualDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(actualDataDaily, DatasetFactory.getTargetSiteTestEncoder());
        Dataset<Row> actualDataDailyRecordsDF = actualDataDailyRecords.toDF();

        String[] domains = {"itrc.ac.ir", "google.com"};

        List<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDaily = Arrays.asList(
                //DataSet For Test PersistRank
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0, IDMaker.getTargetSiteID(domains[0]), 1, 1),
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0, IDMaker.getTargetSiteID(domains[1]), 1, 2),
                new DatasetFactory.DFWithAdditionalColumnTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0, IDMaker.getTargetSiteID(domains[0]), 1, 3)
        );

        Dataset<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(expectedDataDaily, DatasetFactory.getDFWithAdditionalColumnTestEncoder());
        Dataset<Row> expectedDataDailyRecordsDF = expectedDataDailyRecords.toDF();

        RanksAndMeasures instance2 = new RanksAndMeasures();

        Dataset<Row> actualDF = instance2.persistRank2(actualDataDailyRecordsDF, 1);

        Dataset<Row> actualDF1 = expectedDataDailyRecordsDF.select(col("dailySessionCount"),
                col("dailySessionDuration"), col("dailyVisitCount"),
                col("dailyVisitorCount"), col("day"), col("domain"), col("rankScore"),
                col("targetSiteID"), col("logTypeID"), col("rankNum"));
//        actualDF.show();
//        actualDF1.show();

//        DataFiles.saveDailyVisitRankPath(actualDF);
//        DataFiles.saveTargetSitePath(actualDF1);
        assert contentOfDataFramesIsEqual(actualDF1, actualDF);

    }

    public void IncorcetValueOfDailySessionCount0() {

        List<DatasetFactory.targetSiteTest> actualDataDaily = Arrays.asList(
                new DatasetFactory.targetSiteTest(0, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 1.0), //DataSet For Test PersistRank
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0)
        );

        Dataset<DatasetFactory.targetSiteTest> actualDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(actualDataDaily, DatasetFactory.getTargetSiteTestEncoder());
        Dataset<Row> actualDataDailyRecordsDF = actualDataDailyRecords.toDF();

        String[] domains = {"itrc.ac.ir", "google.com"};

        List<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDaily = Arrays.asList(
                //DataSet For Test PersistRank
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0, IDMaker.getTargetSiteID(domains[0]), 1, 1),
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0, IDMaker.getTargetSiteID(domains[1]), 1, 2),
                new DatasetFactory.DFWithAdditionalColumnTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0, IDMaker.getTargetSiteID(domains[0]), 1, 3)
        );

        Dataset<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(expectedDataDaily, DatasetFactory.getDFWithAdditionalColumnTestEncoder());
        Dataset<Row> expectedDataDailyRecordsDF = expectedDataDailyRecords.toDF();

        RanksAndMeasures instance2 = new RanksAndMeasures();

        Dataset<Row> actualDF = instance2.persistRank2(actualDataDailyRecordsDF, 1);

        Dataset<Row> actualDF1 = expectedDataDailyRecordsDF.select(col("dailySessionCount"),
                col("dailySessionDuration"), col("dailyVisitCount"),
                col("dailyVisitorCount"), col("day"), col("domain"), col("rankScore"),
                col("targetSiteID"), col("logTypeID"), col("rankNum"));
//        actualDF.show();
//        actualDF1.show();

//        DataFiles.saveDailyVisitRankPath(actualDF);
//        DataFiles.saveTargetSitePath(actualDF1);
        assert contentOfDataFramesIsEqual(actualDF1, actualDF);
    }

    @Test
    public void incorrectValueOfDailySessionDuration0() {

        List<DatasetFactory.targetSiteTest> actualDataDaily = Arrays.asList(
                new DatasetFactory.targetSiteTest(1554000043, 0, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 1.0), //DataSet For Test PersistRank
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0)
        );

        Dataset<DatasetFactory.targetSiteTest> actualDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(actualDataDaily, DatasetFactory.getTargetSiteTestEncoder());
        Dataset<Row> actualDataDailyRecordsDF = actualDataDailyRecords.toDF();

        String[] domains = {"itrc.ac.ir", "google.com"};

        List<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDaily = Arrays.asList(
                //DataSet For Test PersistRank
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0, IDMaker.getTargetSiteID(domains[0]), 1, 1),
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0, IDMaker.getTargetSiteID(domains[1]), 1, 2),
                new DatasetFactory.DFWithAdditionalColumnTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0, IDMaker.getTargetSiteID(domains[0]), 1, 3)
        );

        Dataset<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(expectedDataDaily, DatasetFactory.getDFWithAdditionalColumnTestEncoder());
        Dataset<Row> expectedDataDailyRecordsDF = expectedDataDailyRecords.toDF();

        RanksAndMeasures instance2 = new RanksAndMeasures();

        Dataset<Row> actualDF = instance2.persistRank2(actualDataDailyRecordsDF, 1);

        Dataset<Row> actualDF1 = expectedDataDailyRecordsDF.select(col("dailySessionCount"),
                col("dailySessionDuration"), col("dailyVisitCount"),
                col("dailyVisitorCount"), col("day"), col("domain"), col("rankScore"),
                col("targetSiteID"), col("logTypeID"), col("rankNum"));
//        actualDF.show();
//        actualDF1.show();

//        DataFiles.saveDailyVisitRankPath(actualDF);
//        DataFiles.saveTargetSitePath(actualDF1);
        assert contentOfDataFramesIsEqual(actualDF1, actualDF);

    }

    @Test
    public void incorrectValueOfDailyVisitCount0() {

        List<DatasetFactory.targetSiteTest> actualDataDaily = Arrays.asList(
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 0, 1454000043, 1454000043, "itrc.ac.ir", 1.0), //DataSet For Test PersistRank
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0)
        );

        Dataset<DatasetFactory.targetSiteTest> actualDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(actualDataDaily, DatasetFactory.getTargetSiteTestEncoder());
        Dataset<Row> actualDataDailyRecordsDF = actualDataDailyRecords.toDF();

        String[] domains = {"itrc.ac.ir", "google.com"};

        List<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDaily = Arrays.asList(
                //DataSet For Test PersistRank
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0, IDMaker.getTargetSiteID(domains[0]), 1, 1),
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0, IDMaker.getTargetSiteID(domains[1]), 1, 2),
                new DatasetFactory.DFWithAdditionalColumnTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0, IDMaker.getTargetSiteID(domains[0]), 1, 3)
        );

        Dataset<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(expectedDataDaily, DatasetFactory.getDFWithAdditionalColumnTestEncoder());
        Dataset<Row> expectedDataDailyRecordsDF = expectedDataDailyRecords.toDF();

        RanksAndMeasures instance2 = new RanksAndMeasures();

        Dataset<Row> actualDF = instance2.persistRank2(actualDataDailyRecordsDF, 1);

        Dataset<Row> actualDF1 = expectedDataDailyRecordsDF.select(col("dailySessionCount"),
                col("dailySessionDuration"), col("dailyVisitCount"),
                col("dailyVisitorCount"), col("day"), col("domain"), col("rankScore"),
                col("targetSiteID"), col("logTypeID"), col("rankNum"));
//        actualDF.show();
//        actualDF1.show();

//        DataFiles.saveDailyVisitRankPath(actualDF);
//        DataFiles.saveTargetSitePath(actualDF1);
        assert contentOfDataFramesIsEqual(actualDF1, actualDF);

    }

    @Test
    public void incorrectValueOfDailyVisitorCount0() {

        List<DatasetFactory.targetSiteTest> actualDataDaily = Arrays.asList(
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 0, 1454000043, "itrc.ac.ir", 1.0), //DataSet For Test PersistRank
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0)
        );

        Dataset<DatasetFactory.targetSiteTest> actualDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(actualDataDaily, DatasetFactory.getTargetSiteTestEncoder());
        Dataset<Row> actualDataDailyRecordsDF = actualDataDailyRecords.toDF();

        String[] domains = {"itrc.ac.ir", "google.com"};

        List<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDaily = Arrays.asList(
                //DataSet For Test PersistRank
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0, IDMaker.getTargetSiteID(domains[0]), 1, 1),
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0, IDMaker.getTargetSiteID(domains[1]), 1, 2),
                new DatasetFactory.DFWithAdditionalColumnTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0, IDMaker.getTargetSiteID(domains[0]), 1, 3)
        );

        Dataset<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(expectedDataDaily, DatasetFactory.getDFWithAdditionalColumnTestEncoder());
        Dataset<Row> expectedDataDailyRecordsDF = expectedDataDailyRecords.toDF();

        RanksAndMeasures instance2 = new RanksAndMeasures();

        Dataset<Row> actualDF = instance2.persistRank2(actualDataDailyRecordsDF, 1);

        Dataset<Row> actualDF1 = expectedDataDailyRecordsDF.select(col("dailySessionCount"),
                col("dailySessionDuration"), col("dailyVisitCount"),
                col("dailyVisitorCount"), col("day"), col("domain"), col("rankScore"),
                col("targetSiteID"), col("logTypeID"), col("rankNum"));
//        actualDF.show();
//        actualDF1.show();

//        DataFiles.saveDailyVisitRankPath(actualDF);
//        DataFiles.saveTargetSitePath(actualDF1);
        assert contentOfDataFramesIsEqual(actualDF1, actualDF);

    }

    @Test
    public void incorrectValueOfRankScore0() {

        List<DatasetFactory.targetSiteTest> actualDataDaily = Arrays.asList(
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 0.0), //DataSet For Test PersistRank
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0)
        );

        Dataset<DatasetFactory.targetSiteTest> actualDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(actualDataDaily, DatasetFactory.getTargetSiteTestEncoder());
        Dataset<Row> actualDataDailyRecordsDF = actualDataDailyRecords.toDF();

        String[] domains = {"itrc.ac.ir", "google.com"};

        List<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDaily = Arrays.asList(
                //DataSet For Test PersistRank
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0, IDMaker.getTargetSiteID(domains[0]), 1, 1),
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0, IDMaker.getTargetSiteID(domains[1]), 1, 2),
                new DatasetFactory.DFWithAdditionalColumnTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0, IDMaker.getTargetSiteID(domains[0]), 1, 3)
        );

        Dataset<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(expectedDataDaily, DatasetFactory.getDFWithAdditionalColumnTestEncoder());
        Dataset<Row> expectedDataDailyRecordsDF = expectedDataDailyRecords.toDF();

        RanksAndMeasures instance2 = new RanksAndMeasures();

        Dataset<Row> actualDF = instance2.persistRank2(actualDataDailyRecordsDF, 1);

        Dataset<Row> actualDF1 = expectedDataDailyRecordsDF.select(col("dailySessionCount"),
                col("dailySessionDuration"), col("dailyVisitCount"),
                col("dailyVisitorCount"), col("day"), col("domain"), col("rankScore"),
                col("targetSiteID"), col("logTypeID"), col("rankNum"));
//        actualDF.show();
//        actualDF1.show();

//        DataFiles.saveDailyVisitRankPath(actualDF);
//        DataFiles.saveTargetSitePath(actualDF1);
        assert contentOfDataFramesIsEqual(actualDF1, actualDF);

    }

    @Test
    public void mixedIncorrectInput() {

        List<DatasetFactory.targetSiteTest> actualDataDaily = Arrays.asList(
                new DatasetFactory.targetSiteTest(1554000043, 0, 1454000043, -10, 1454000043, "itrc.ac.ir", -7.0), //DataSet For Test PersistRank
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0)
        );

        Dataset<DatasetFactory.targetSiteTest> actualDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(actualDataDaily, DatasetFactory.getTargetSiteTestEncoder());
        Dataset<Row> actualDataDailyRecordsDF = actualDataDailyRecords.toDF();

        String[] domains = {"itrc.ac.ir", "google.com"};

        List<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDaily = Arrays.asList(
                //DataSet For Test PersistRank
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0, IDMaker.getTargetSiteID(domains[0]), 1, 1),
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0, IDMaker.getTargetSiteID(domains[1]), 1, 2),
                new DatasetFactory.DFWithAdditionalColumnTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2.0, IDMaker.getTargetSiteID(domains[0]), 1, 3)
        );

        Dataset<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(expectedDataDaily, DatasetFactory.getDFWithAdditionalColumnTestEncoder());
        Dataset<Row> expectedDataDailyRecordsDF = expectedDataDailyRecords.toDF();

        RanksAndMeasures instance2 = new RanksAndMeasures();

        Dataset<Row> actualDF = instance2.persistRank2(actualDataDailyRecordsDF, 1);

        Dataset<Row> actualDF1 = expectedDataDailyRecordsDF.select(col("dailySessionCount"),
                col("dailySessionDuration"), col("dailyVisitCount"),
                col("dailyVisitorCount"), col("day"), col("domain"), col("rankScore"),
                col("targetSiteID"), col("logTypeID"), col("rankNum"));
//        actualDF.show();
//        actualDF1.show();

//        DataFiles.saveDailyVisitRankPath(actualDF);
//        DataFiles.saveTargetSitePath(actualDF1);
        assert contentOfDataFramesIsEqual(actualDF1, actualDF);

    }

    @Test
    public void repeatedScore() {

        List<DatasetFactory.targetSiteTest> actualDataDaily = Arrays.asList(
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 1.0), //DataSet For Test PersistRank
                new DatasetFactory.targetSiteTest(1554000043, 1454000043, 1454000043, 1454000043, 1564000043, "itrc.ac.ir", 1.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0),
                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0)
        );

        Dataset<DatasetFactory.targetSiteTest> actualDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(actualDataDaily, DatasetFactory.getTargetSiteTestEncoder());
        Dataset<Row> actualDataDailyRecordsDF = actualDataDailyRecords.toDF();

        String[] domains = {"itrc.ac.ir", "google.com"};

        List<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDaily = Arrays.asList(
                //DataSet For Test PersistRank
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4.0, IDMaker.getTargetSiteID(domains[0]), 1, 1),
                new DatasetFactory.DFWithAdditionalColumnTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3.0, IDMaker.getTargetSiteID(domains[1]), 1, 2),
                new DatasetFactory.DFWithAdditionalColumnTest(1554000043, 1454000043, 1454000043, 1454000043, 1564000043, "itrc.ac.ir", 1.0, IDMaker.getTargetSiteID(domains[0]), 1, 3),
                new DatasetFactory.DFWithAdditionalColumnTest(1554000043, 1454000043, 1454000043, 1454000043, 1564000043, "itrc.ac.ir", 1.0, IDMaker.getTargetSiteID(domains[0]), 1, 4)
        );

        Dataset<DatasetFactory.DFWithAdditionalColumnTest> expectedDataDailyRecords
                = SparkHandlers.getSparkSession().createDataset(expectedDataDaily, DatasetFactory.getDFWithAdditionalColumnTestEncoder());
        Dataset<Row> expectedDataDailyRecordsDF = expectedDataDailyRecords.toDF();

        RanksAndMeasures instance2 = new RanksAndMeasures();

        Dataset<Row> actualDF = instance2.persistRank2(actualDataDailyRecordsDF, 1);

        Dataset<Row> actualDF1 = expectedDataDailyRecordsDF.select(col("dailySessionCount"),
                col("dailySessionDuration"), col("dailyVisitCount"),
                col("dailyVisitorCount"), col("day"), col("domain"), col("rankScore"),
                col("targetSiteID"), col("logTypeID"), col("rankNum"));
//        actualDF.show();
//        actualDF1.show();

//        DataFiles.saveDailyVisitRankPath(actualDF);
//        DataFiles.saveTargetSitePath(actualDF1);
//        assert contentOfDataFramesIsEqual(actualDF1, actualDF);
    }

}

//     @Test
//     public void nullTest(){
//     
//     
//     
//     }

