package ir.ac.itrc.rotbenegar.Ranking;

import ir.ac.itrc.rotbenegar.Pipeline.LogProcessing;
import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import java.io.BufferedReader;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.apache.spark.sql.types.DataTypes.DateType;

public class LogRank extends Score {

    private long pageViews;
    private int uniqueVisitors;
    private long totalSessions;
    private long sumSessionsDuration;
    private float avgSessionDuration;
    private long score;
    public User unnamed_User_;
    SparkHandlers spark = new SparkHandlers();

    /**
     * input : JavaRDD<timeStamp,user-ID>
     */
    public Dataset<Row> computeScore(Dataset<Row> records) {

        final long startTime = System.currentTimeMillis();
      records=  records.withColumnRenamed("domain", "destination")
              .withColumnRenamed("userIP","user_ip")
              
              ;
//                .withColumnRenamed("", "")
                ;
        Dataset<Row> df = records;
        WindowSpec partitionWindow = partitioning();

        Column LagTimeStamp = functions.lag(df.col("TimeStamp"), 1, -1).over(partitionWindow);
        df = df.withColumn("LagTimeStamp", LagTimeStamp);
        Column diffBack = when(df.col("LagTimeStamp").$less(0), 25).otherwise(df.col("TimeStamp").minus(LagTimeStamp.as("LagTimeStamp")));

//        Column countTest = functions.count("dest_port").over(partitionWindow);
        Column session = (coalesce(
                (col("TimeStamp").$minus(lag("TimeStamp", 1, 0).over(partitionWindow)))
        ).$greater(1800)).cast(IntegerType);

        Dataset<Row> sessionized = df.withColumn("session", functions.sum(session).over(partitionWindow));
//        System.out.println("sessionized.show()");
//        sessionized.show(150);
        Dataset<Row> df2 = sessionized.withColumn("diffBack", diffBack);
//        System.out.println("df2.show()1");
//        df2.show(150);

        //uniqu visitor
        Dataset<Row> uv = super.calculateUniqueVisitors(df2);
//        System.out.println("uv.show();");
//        uv.show();
        //page view
        Dataset<Row> pv = super.calculatePageViews(df2);
//        System.out.println("pv.show();");
//        pv.show();

        //total session
        Dataset<Row> sessionCount = calculateSessionCount(df2);
//        Dataset<Row> sessionCount = df2.groupBy("user_ip", "destination").agg(functions.count(df2.col("session")).as("sessionCount"));
//        System.out.println("sessionCount.show()");
//        sessionCount.show();
        Dataset<Row> totalSession = calculateTotalSession(sessionCount);
//        Dataset<Row> totalSession = sessionCount.groupBy("destination").agg(functions.sum("sessionCount").as("totalSession"));
//        System.out.println("totalSession.show()");
//        totalSession.show();
//        System.out.println("df2.show()2");
//        df2.show();
        //sum-session-duration
        Dataset<Row> sumSessionDuration = calculateSumSessionDuration(df2);
//        Dataset<Row> sumSessionDuration = df2.groupBy("destination").agg(functions.sum(df2.col("diffBack")).as("sumSessionDuration"));
//        System.out.println("sumSessionDuration.show()");
//        sumSessionDuration.show();

//        Dataset<Row> joined = uv.join(pv, ("destination")).join(sumSessionDuration, "destination").as("sessions").join(totalSession, "destination");
//        Dataset<Row> joined = uv.join(pv, ("destination")).join(sumSessionDuration, "destination").as("sessions").join(totalSession, "destination");
        Dataset<Row> joined = uv.join(pv, uv.col("uvday").equalTo(pv.col("pvday")).and(uv.col("uvhost").equalTo(pv.col("pvhost"))), "left");
        joined = joined.drop("pvday").drop("pvhost");
        joined = joined.join(sumSessionDuration, joined.col("uvday").equalTo(sumSessionDuration.col("sumSessionDurationday")).and(joined.col("uvhost").equalTo(sumSessionDuration.col("sumSessionDurationhost"))), "left");
        joined = joined.drop("sumSessionDurationday").drop("sumSessionDurationhost");
        joined = joined.join(totalSession, joined.col("uvday").equalTo(totalSession.col("totalSessionday")).and(joined.col("uvhost").equalTo(totalSession.col("totalSessionhost"))), "left");
        joined = joined.drop("totalSessionday").drop("totalSessionhost");

        joined = calculateAvg(joined);
//        System.out.println("joined.show()2");
//        joined.show();

        joined = calculateScore(joined);
//        System.out.println("joined.show()3");
//        joined.show();
        joined = renameColumns(joined);
//        joined.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/ammar/Downloads/lessClean100daysResult.csv");

        final long endTime = System.currentTimeMillis();
        System.out.println("Total execution timecomputeScore: " + (endTime - startTime) / 1000 + " seconds");
        return joined.drop(col("avg"));
    }

    private Dataset<Row> calculateSessionCount(Dataset<Row> records) {
        
        return records.groupBy("day", "user_ip", "destination").agg(functions.countDistinct(records.col("session")).as("sessionCount"));
    }

    private Dataset<Row> calculateTotalSession(Dataset<Row> records) {
        return records.groupBy("day", "destination").agg(functions.sum("sessionCount").as("totalSession")).withColumnRenamed("destination", "totalSessionhost").withColumnRenamed("day", "totalSessionday");
    }

    private Dataset<Row> calculateSumSessionDuration(Dataset<Row> records) {
        return records.groupBy("day", "destination").agg(functions.sum(records.col("diffBack")).as("sumSessionDuration")).withColumnRenamed("destination", "sumSessionDurationhost").withColumnRenamed("day", "sumSessionDurationday");
    }

    private Dataset<Row> calculateAvg(Dataset<Row> records) {
        return records.withColumn("avg", records.col("sumSessionDuration").divide(records.col("totalSession")));
    }

    private Dataset<Row> calculateScore(Dataset<Row> records) {
        return records.withColumn("score", (records.col("uniqueVisitors").$plus(records.col("totalSession"))).divide(2).multiply(records.col("sumSessionDuration")));
    }

    private Dataset<Row> renameColumns(Dataset<Row> records) {
        return records.withColumnRenamed("uvhost", "domain")
                .withColumnRenamed("uvday", "day")
                .withColumnRenamed("pageViews", "dailyVisitCount")
                .withColumnRenamed("uniqueVisitors", "dailyVisitorCount")
                .withColumnRenamed("totalSession", "dailySessionCount")
                .withColumnRenamed("session", "sumSessionDuration")
                .withColumnRenamed("score", "rankScore");
        
    }

    /**
     * input : JavaRDD<timestamp,user-ID,diff-back,session>
     */
  

    /**
     * input : JavaRDD<timestamp,user-ID,diff-back,session>
     */
    private WindowSpec partitioning() {
        return Window.partitionBy("day", "destination", "user_ip").orderBy("TimeStamp");
    }

    /**
     * input : JavaRDD<timestamp,user-ID,diff-back,session>
     */
    private long calculateDailyTotalSessions(Dataset<Row> records) {
        throw new UnsupportedOperationException();
    }

    /**
     * input : JavaRDD<diff-back>
     */
    private long calculateSumSessionDuration222(Dataset<Row> records) {
        throw new UnsupportedOperationException();
    }

    private float calculateAvgSessionDuration(long sumSessionsDuration, long totalSessions) {
        throw new UnsupportedOperationException();
    }

    private float scoreFunc(Object uniqueVisitors, Object totalSessions, Object sumSessionsDuration) {
        throw new UnsupportedOperationException();
    }
}
