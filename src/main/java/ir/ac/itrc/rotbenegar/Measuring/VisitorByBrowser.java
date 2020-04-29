package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.DataFormats.CriterionType;
import java.io.IOException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;
import org.spark_project.jetty.util.ArrayUtil;

public class VisitorByBrowser extends VisitorByCriterion {

	public VisitorByBrowser() {
		super(CriterionType.BROWSER);
	}

	/**
	 * input: JavaRDD<browser, os, referrerType, user_ip, user_ID>
	 * Note1: order of fields matters unless fields have column names
	 * 
	 * output: JavaRDD<criterion_fld, user_ID_fld>
	 * Note2: 'criterion_fld' represents either of browser, os, referrerType, or user_ip fields.
	 */
	protected Dataset<Row> prepareData(Dataset<Row> records) {
            if (ArrayUtils.contains(records.columns(), Measures.DataFieldsName.browser.name()) && 
                    ArrayUtils.contains(records.columns(), Measures.DataFieldsName.userID.name())) {
                records = records.withColumn("day", regexp_replace(col("day"), "-", ""));
                return records;
            }
            
            return null;
	}
}
