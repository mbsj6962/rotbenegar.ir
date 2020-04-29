/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Pipeline;

import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

/**
 *
 * @author ammar
 */
public class Fraud {
String userIP = "userIP";
String fraud_ip ="fraud_ip";
String left_outer="left_outer";

    public Dataset<Row> fraudDetection(Dataset<Row> records) {
        String path;
        DataFiles datafile = new DataFiles();
        path = datafile.getFraudFilePath();
//joined: 1265784
//records:1265812

//records:1265812
//joined: 1265668
        //read file
        Dataset<Row> df = read(path);
//        records.show();
//        df.show();
//        System.out.println("records:" + records.count());
//        System.out.println("dfs:" + df.count());
//        records = records.except(df);
//        WindowSpec partitionWindow = partitioning();
        Dataset<Row> joined = records.join(df, records.col(userIP).equalTo(df.col(fraud_ip)), left_outer);
//        joined.show();
        joined = joined.where(joined.col(fraud_ip).isNull());
//        joined.show();
//        System.out.println("joined:" + joined.count());
        return joined;
    }

    private Dataset<Row> read(String path) {
        //read csv file
        Dataset<Row> df = SparkHandlers.getSparkSession().read()
                .option("header", true)
                //                .option("delimiter", "\t")
                .csv(path);
        return df;
    }

    /**
     * input : JavaRDD<timestamp,user-ID,diff-back,session>
     */
    private WindowSpec partitioning() {
        return Window.partitionBy(userIP);
    }

}
