/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Ranking;

import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import scala.collection.Seq;

/**
 *
 * @author ammar
 */
public class ThreeMonthsRanking {

    public void calculate(String path) {
        Dataset<Row> df = null;
        df = read("");
//        df.show();
//        df.printSchema();
        // cast int to date
        df = df.withColumn("date", functions.concat(
                functions.col("date").substr(0, 4), lit("-"), functions.col("date").substr(5, 2),
                 lit("-"), functions.col("date").substr(7, 2)));

        df = df.withColumn("date", df.col("date").cast(TimestampType)/*.cast(DateType)*/);

        Dataset<Row> pastDays = df.select("date").distinct().orderBy("date");
        Dataset<Row> today = df.agg(max("date")).withColumnRenamed("max(date)", "date");
        Dataset<Row> from = today.select(date_add(today.col("date"), -2).cast(TimestampType)).withColumnRenamed("CAST(date_add(date, -2) AS TIMESTAMP)", "date");
        df = df.select("targetSiteID", "rankScore", "date"); //.where(col("date").$greater$eq(lit(from)));

        df.select("date").distinct().orderBy("date").show();
//        df = df.filter(df.col("date").$less(from));
//        data.filter(data("date").gt(lit("2015-03-14")))
        df.select("date").distinct().orderBy("date").show();

        //past days
        //select domain, score, score_date
        //
        //total days
        int totalDays = 30;
        //select formula
        df = df.groupBy("targetSiteID").agg(
                sum("rankScore")
                .minus((avg("rankScore").multiply((count("rankScore").minus(totalDays)))))
                .plus((count("rankScore").divide(totalDays).multiply(0.1)))
//                sum("rankScore"),
//                count("rankScore"),
//                avg("rankScore"),
//                (avg("rankScore").multiply(abs(count("rankScore").minus(totalDays)))),
//                (count("rankScore").divide(totalDays).multiply(0.1)),

//                sum("rankScore")
//                .plus((avg("rankScore").multiply(abs(count("rankScore").minus(totalDays)))))
//                .plus((count("rankScore").divide(totalDays).multiply(0.1))),
        ).withColumnRenamed("((sum(rankScore) - (avg(rankScore) * (count(rankScore) - 30))) + ((count(rankScore) / 30) * 0.1))", "monthlyScore");
        df = df.orderBy(desc("monthlyScore"));
        WindowSpec w = Window.orderBy(desc("monthlyScore"));
//        df.select("targetSiteID", rank().over(w).alias("rank"));
//        df = df.withColumn("rank", rank().over().w over w);
        df = df.withColumn("rank", row_number().over(w));
        df.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/ammar/Downloads/3Months.csv");

//        df.withColumn("rank", row_number().over(Window.partitionBy($"accountNumber").orderBy($"assetValue".desc))).Printers$class.show($this, w, printTypes, printIds, printOwners, printKinds, printMirrors, printPositions)
    

//        df = df.groupBy("targetSiteID").agg(sum("rankScore")
//                .plus((avg("rankScore").multiply((count("rankScore").minus(totalDays)))))
//                .plus((count("rankScore").divide(totalDays).multiply(0.1))));
//        df.show(false);

        //
        //sort desc
        //rank
    }

    /**
     * output: JavaRDD<records>
     */
    public Dataset<Row> read(String path) {
        Dataset<Row> df = SparkHandlers.getSQLContext().read().format("jdbc").
                option("url", "jdbc:mysql://localhost:3306/webrankingormv5beNEW").
                //                option("url", "jdbc:mysql://localhost:3306/webrankingormv5beNEW?zeroDateTimeBehavior=convertToNull").
                option("driver", "com.mysql.jdbc.Driver").
                option("useUnicode", "true").
                option("continueBatchOnError", "true").
                option("useSSL", "false").
                option("user", "root").
                option("password", "root").
                option("dbtable", "daily_visit_rank").
                load();
        return df;
    }
}