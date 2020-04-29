package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.DataFormats.CriterionType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class VisitorByLocation extends VisitorByCriterion {

	public VisitorByLocation() {
		super(CriterionType.LOCATION);
	}
        
	/**
	 * input: JavaRDD<browser, os, referrerType, user_ip, user_ID>
	 * Note1: order of fields matters unless fields have column names
	 * 
	 * output: JavaRDD<criterion_fld, user_ID_fld>
	 * Note2: 'criterion_fld' represents either of browser, os, referrerType, or user_ip fields.
	 */
	protected Dataset<Row> prepareData(Dataset<Row> records) {
		throw new UnsupportedOperationException();
	}

}