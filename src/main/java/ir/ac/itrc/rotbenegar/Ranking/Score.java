package ir.ac.itrc.rotbenegar.Ranking;

import ir.ac.itrc.rotbenegar.Pipeline.RanksAndMeasures;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public abstract class Score {

    public RanksAndMeasures unnamed_RanksAndMeasures_;

    /**
     * input : Dataset<Row>
     */
    protected Dataset<Row> calculateUniqueVisitors(Dataset<Row> records) {
        return records.groupBy("day","destination").agg(functions.countDistinct(records.col("user_ip")).as("uniqueVisitors")).withColumnRenamed("destination", "uvhost").withColumnRenamed("day", "uvday");
//        empsalary.withColumn("avg", avg('salary) over byDepName).show
    }

    /**
     * input : Dataset<Row>
     */
    protected Dataset<Row> calculatePageViews(Dataset<Row> records) {
        return records.groupBy("day","destination").agg(functions.count(records.col("destination")).as("pageViews")).withColumnRenamed("destination", "pvhost").withColumnRenamed("day", "pvday");
    }

    protected void persistSiteVisit(Object targetSiteID, Object logTypeID, Object pageViews, Object uniqueVisitors, Object day, Object totalSessions, Object sumSessionsDuration) {
        throw new UnsupportedOperationException();
    }

    /**
     * input records: JavaRDD<timeStamp,user-ID>
     *
     * output:
     * JavaRDD<score, dailyVisitCount, dailyVisitorCount, dailySessionCount, dailySessionDuration>
     */
    public Dataset<Row> computeScore(Dataset<Row> records) {
        throw new UnsupportedOperationException();
    }
}
