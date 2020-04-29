/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Utilities;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

/**
 *
 * @author hhduser
 */
public class utils {

    public static Dataset<Row> RemoveDashes(Dataset<Row> records) {

        records = records.withColumn("day", regexp_replace(col("day"), "-", ""));

        return records;

    }

}
