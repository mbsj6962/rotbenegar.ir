package ir.ac.itrc.rotbenegar.Pipeline;
//

import com.google.common.hash.HashCode;
import ir.ac.itrc.rotbenegar.DataFormats.ReturnDataSets;
import ir.ac.itrc.rotbenegar.Measuring.Measures;
import ir.ac.itrc.rotbenegar.Ranking.LogRank;
import ir.ac.itrc.rotbenegar.Ranking.Score;
import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import ir.ac.itrc.rotbenegar.Utilities.IDMaker;
import ir.ac.itrc.rotbenegar.Utilities.Logger;
import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import ir.ac.itrc.rotbenegar.Utilities.UserAgent;
import java.text.ParseException;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Locale;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.spark_project.guava.net.InternetDomainName;

public class RanksAndMeasures implements java.io.Serializable {

//    private static SparkSession SparkSession;
    Measures dailyMeasure;
    String sumSessionDuration ="sumSessionDuration";
    String dailySessionCount = "dailySessionCount";
    String dailySessionDuration = "dailySessionDuration";
    String dailyVisitCount = "dailyVisitCount";
    String dailyVisitorCount = "dailyVisitorCount";
    String day = "day";
    String rankScore = "rankScore";
    String targetSiteID = "targetSiteID";
    String logTypeID = "logTypeID";
    String rankNum ="rankNum";
    String domain ="domain";
    /**
     * JavaRDD<Measures>
     */
    public Dataset<Row> getTargetSiteDataset(Dataset<Row> dailyRankResult) {
        return dailyRankResult.select(col(targetSiteID), col(domain));
    }

    public void persistMeasures(Dataset<Row> records, int logTypeID) {

        dailyMeasure = new Measures();
        dailyMeasure.persistMeasures(records, logTypeID);
    }

    private HashCode getTargetSiteID(String targetSiteName) {

        throw new UnsupportedOperationException();
    }

    /**
     * @param records JavaRDD<Tuple(targetSiteID, score)>
     */
    private Dataset<Row> computeRank(Dataset<Row> records) {
        throw new UnsupportedOperationException();
    }

//    private int getRankFormulaID(String rankFormulaDescription) throws Exception {
//        int result;
//        if (rankFormulaDescription.equals("LogRank")) {
//            result = 1;
//        } else {
//            throw new Exception("ERROR : " + rankFormulaDescription + " Folrmula is not defined");
//        }
//        return result;
//    }
    private int getRankFormulaID(String rankFormulaDescription) {
        if (rankFormulaDescription.equals("LogRank")) {
            return 1;
        } else {
            return -1;
        }
    }

    public ReturnDataSets persistRank(Dataset<Row> records, String rankFormulaDescription, int logTypeID) throws Exception {//newRec==scroeResult

//        SparkSession spark = SparkHandlers.getSparkSession();
        int rankFormulaID = getRankFormulaID(rankFormulaDescription);
        Score logRank;
        if (rankFormulaID == 1) {
            logRank = new LogRank();
        } else {
            throw new Exception("ERROR : " + rankFormulaDescription + " Folrmula is not defined");
        }

        Dataset<Row> newRec = logRank.computeScore(records);
//            Score logRank = new LogRank();
//            Dataset<Row> newRec = logRank.computeScore(records);            

        boolean boolResult = ArrayUtils.contains(newRec.columns(), dailySessionCount)
                && ArrayUtils.contains(newRec.columns(), sumSessionDuration)
                && ArrayUtils.contains(newRec.columns(), dailyVisitCount)
                && ArrayUtils.contains(newRec.columns(), dailyVisitorCount)
                && ArrayUtils.contains(newRec.columns(), day)
                && ArrayUtils.contains(newRec.columns(), domain)
                && ArrayUtils.contains(newRec.columns(), rankScore);
//             newRec.printSchema();
        if (boolResult == false) {
            throw new UnsupportedOperationException();
        }

        // Sets of Checking -> Enable to Delete these Checking Its Up to YOU!!!
        Dataset<Row> JUnitChecking = newRec.filter(col(dailySessionCount).gt(0)).filter(col(sumSessionDuration).gt(0))
                .filter(col(dailyVisitCount).gt(0)).filter(col(dailyVisitorCount).gt(0)).filter(col(rankScore).gt(0));

//        Dataset<Row> JUnitChecking1 = newRec.filter(col("dailySessionCount").leq(0)).filter(col("sumSessionDuration").leq(0))
//                .filter(col("dailyVisitCount").leq(0)).filter(col("dailyVisitorCount").leq(0)).filter(col("rankScore").leq(0));
//
//        Dataset<Row> JUnitChecking2 = newRec.filter(col("dailySessionCount").isNull()).filter(col("sumSessionDuration").isNull())
//                .filter(col("dailyVisitCount").isNull()).filter(col("dailyVisitorCount").isNull()).filter(col("rankScore").isNull());
//        DataFiles.saveInvalidRecordsEqualZero(JUnitChecking1);
//        DataFiles.saveInvalidRecordsNull(JUnitChecking2);
        ////////
        //1. filter negative & edge tests
        //2. filter invalid domain
        ////////
        UDF1 targetSiteID = new UDF1<String, String>() {
            public String call(final String domainName) throws Exception {
                return IDMaker.getTargetSiteID(domainName);
            }
        };

        SparkSession spark = SparkHandlers.getSparkSession();
        spark.udf().register("targetsiteid", targetSiteID, DataTypes.StringType);
        //newRec.select(callUDF("targetsiteid", col("domain")));

        WindowSpec rankNumRecords = Window.partitionBy(col(day)).orderBy(col(rankScore).desc());

        Dataset<Row> newRecordsWithAdditionalColumn;// = 

        newRecordsWithAdditionalColumn
                = JUnitChecking.withColumn(this.targetSiteID, callUDF("targetsiteid", col(domain))). /*Hash Domain*/
                        withColumn(this.logTypeID, lit(logTypeID)).withColumn(rankNum, row_number().over(rankNumRecords)) /*Add logTypeID and rankNum*/
                        .select(col(dailySessionCount), /*select fields needed */
                                col(sumSessionDuration), col(dailyVisitCount),
                                col(dailyVisitorCount), col(day), col(domain), col(rankScore),
                                col(this.targetSiteID), col(this.logTypeID), col(rankNum)).withColumnRenamed(sumSessionDuration, dailySessionDuration);

//        newRecordsWithAdditionalColumn = newRecordsWithAdditionalColumn.withColumnRenamed("sumSessionDuration", "dailySessionDuration");
        newRecordsWithAdditionalColumn.persist();//

        Dataset<Row> IDAndSiteDomainName = newRecordsWithAdditionalColumn.select(col(this.targetSiteID), col(domain));
//        
        Dataset<Row> TargetSiteRank = newRecordsWithAdditionalColumn.drop(domain);

//        UDF1 removeDash = new UDF1< Date, String>() {
//
//            @Override
//            public String call(Date t1) throws Exception {
//                
//                return null;
//            }
//        };
//
//        spark.udf().register("removeDash", removeDash, DataTypes.DateType);
//        TargetSiteRank = TargetSiteRank.withColumn("day", callUDF("removeDash", col("day")));
        TargetSiteRank = TargetSiteRank.withColumn(day, regexp_replace(col(day), "-", ""));

        DataFiles.saveDailyVisitRank(TargetSiteRank);
        DataFiles.saveTargetSite(IDAndSiteDomainName);
//        TargetSiteRank.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/hhduser/ss/Resul10");
//        IDAndSiteDomainName.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/hhduser/ss/Result2");

//        TargetSiteRank.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("/media/hhduser/Amoozesh/ss/Result61B");
        ReturnDataSets myReturn = new ReturnDataSets();
//        myReturn.setTargetSiteIDAndDomain(IDAndSiteDomainName);
        myReturn.setTargetSiteRank(TargetSiteRank);
//        System.in.read();
//        TargetSiteRank.printSchema();
//        System.out.println("count is : ");

//        IDAndSiteDomainName.printSchema();

//        System.out.println("count is : ");
        return myReturn;
    }

    public Dataset<Row> persistRank2(Dataset<Row> newRec, int logTypeID) {//newRec==scroeResult

//        SparkSession spark = SparkHandlers.getSparkSession();
//
//        //TODO: delete later
//        List<DatasetFactory.targetSiteTest> actualDataDaily = Arrays.asList(
//                new DatasetFactory.targetSiteTest(1454000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 1), //DataSet For Test PersistRank
//                new DatasetFactory.targetSiteTest(1454000043, 1454000043, 1454000043, 1454000043, 1454000043, "itrc.ac.ir", 2),
//                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "google.com", 3),
//                new DatasetFactory.targetSiteTest(1553939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 4),
//                new DatasetFactory.targetSiteTest(1653939200, 1454000043, 1554000043, 1454000043, 1454000043, "itrc.ac.ir", 5)
//        );
//
//        Dataset<DatasetFactory.targetSiteTest> actualDataDailyRecords
//                = SparkHandlers.getSparkSession().createDataset(actualDataDaily, DatasetFactory.getTargetSiteTestEncoder());
//        Dataset<Row> actualDataDailyRecordsDF = actualDataDailyRecords.toDF();
//        //end of delete later
//
//        Score score = new LogRank();
//        Dataset<Row> newRec = actualDataDailyRecordsDF;//score.computeScore(records);
        //newRec.printSchema();
        boolean boolResult = ArrayUtils.contains(newRec.columns(), "dailySessionCount")
                && ArrayUtils.contains(newRec.columns(), "dailySessionDuration")
                && ArrayUtils.contains(newRec.columns(), "dailyVisitCount")
                && ArrayUtils.contains(newRec.columns(), "dailyVisitorCount")
                && ArrayUtils.contains(newRec.columns(), "day")
                && ArrayUtils.contains(newRec.columns(), "domain")
                && ArrayUtils.contains(newRec.columns(), "rankScore");
//             newRec.printSchema();
        if (boolResult == false) {
            throw new UnsupportedOperationException();
        }

        // Sets of Checking -> Enable to Delete these Checking Its Up to YOU!!!
        Dataset<Row> JUnitChecking = newRec.filter(col("dailySessionCount").gt(0)).filter(col("dailySessionDuration").gt(0))
                .filter(col("dailyVisitCount").gt(0)).filter(col("dailyVisitorCount").gt(0)).filter(col("rankScore").gt(0));

        Dataset<Row> JUnitChecking1 = newRec.filter(col("dailySessionCount").leq(0)).filter(col("dailySessionDuration").leq(0))
                .filter(col("dailyVisitCount").leq(0)).filter(col("dailyVisitorCount").leq(0)).filter(col("rankScore").leq(0));

        Dataset<Row> JUnitChecking2 = newRec.filter(col("dailySessionCount").isNull()).filter(col("dailySessionDuration").isNull())
                .filter(col("dailyVisitCount").isNull()).filter(col("dailyVisitorCount").isNull()).filter(col("rankScore").isNull());

//        DataFiles.saveInvalidRecordsEqualZero(JUnitChecking1);
//        DataFiles.saveInvalidRecordsNull(JUnitChecking2);
        ////////
        //1. filter negative & edge tests
        //2. filter invalid domain
        ////////
        // it's been Optimized
        UDF1 targetSiteID = new UDF1<String, String>() {
            public String call(final String domainName) throws Exception {
                return IDMaker.getTargetSiteID(domainName);
            }
        };

        SparkSession spark = SparkHandlers.getSparkSession();
        spark.udf().register("targetsiteid", targetSiteID, DataTypes.StringType);
        //newRec.select(callUDF("targetsiteid", col("domain")));

        WindowSpec rankNumRecords = Window.partitionBy(col("day")).orderBy(col("rankScore").desc());

        Dataset<Row> newRecordsWithAdditionalColumn;// = 

        newRecordsWithAdditionalColumn
                = JUnitChecking.withColumn("logTypeID", lit(logTypeID)).withColumn("targetSiteID",
                        callUDF("targetsiteid", col("domain"))).withColumn("rankNum", row_number().over(rankNumRecords));
        Dataset<Row> AcceptableRecords = newRecordsWithAdditionalColumn.select(col("dailySessionCount"),
                col("dailySessionDuration"), col("dailyVisitCount"),
                col("dailyVisitorCount"), col("day"), col("domain"), col("rankScore"),
                col("targetSiteID"), col("logTypeID"), col("rankNum"));

//            DataFiles.saveDailyVisitRankPath(AcceptableRecords);
//            Dataset<Row> IDAndSiteDomainName = AcceptableRecords.select(col("targetSiteID"), col("domain"));
//            DataFiles.saveTargetSitePath(IDAndSiteDomainName);
        return AcceptableRecords;
    }

}
