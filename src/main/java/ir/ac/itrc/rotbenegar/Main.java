package ir.ac.itrc.rotbenegar;

import ir.ac.itrc.rotbenegar.DataFormats.DatasetFactory;
import ir.ac.itrc.rotbenegar.Pipeline.LogProcessing;
import ir.ac.itrc.rotbenegar.Ranking.LogRank;
import ir.ac.itrc.rotbenegar.Ranking.Score;
import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import static ir.ac.itrc.rotbenegar.Utilities.DataFiles.getVisitorByCityPath;
import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

public class Main {

    public static void main(String[] args) {
//        System.out.println("Hello World!");
//        List<DatasetFactory.DataFields> data = Arrays.asList(
//                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "chrome", "winxp", "Link"),
//                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "chrome", "winxp", "Link"),
//                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", "safari", "winxp", "Link"),
//                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "android", "winxp", "Link")
//        );

         Dataset<Row> df = SparkHandlers.getSparkSession().read()
                .option("header", true)
                .option("delimiter", "\t")
                .csv("/home/hhduser/Downloads/3005_1000000_7492.csv.lzo");
                
         df.printSchema();
         
df.select(col("start_time")).write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save("/home/hhduser/outputTestLzo");         
         DataFiles.saveDailyVisitRank(df);
            
        
//        Score logRank = new LogRank();
//        Dataset<Row> df = null;
//        logRank.computeScore(df).show();

        LogProcessing logProcessing = new LogProcessing("1");
    }
}
