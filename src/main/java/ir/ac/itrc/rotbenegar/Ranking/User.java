package ir.ac.itrc.rotbenegar.Ranking;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class User {
	public LogRank unnamed_LogRank_;
	public LogRank unnamed_LogRank_2;

	/**
	 * input : JavaRDD<timestamp,user-ID>
	 * output : JavaRDD<timestamp,user-ID,diff-back,session>
	 */
	public Dataset<Row> calculateDiffBackAndSession(Dataset<Row> records) {
		throw new UnsupportedOperationException();
	}

	/**
	 * input : JavaRDD<timestamp,user-ID>
	 * output : JavaRDD<timestamp,user-ID>
	 */
	private Dataset<Row> sortAscByTime(Dataset<Row> records) {
		throw new UnsupportedOperationException();
	}

	/**
	 * input : JavaRDD<timestamp,user-ID>
	 * output : JavaRDD<timestamp,user-ID,diff-back,session>
	 */
	private Dataset<Row> fillRDD(Dataset<Row> records) {
		throw new UnsupportedOperationException();
	}

	/**
	 * input : JavaRDD<timestamp,user-ID,diff-back,session>
	 * output : JavaRDD<user-ID,sessionCount>
	 */
	public Dataset<Row> sessionCountCalc(Dataset<Row> records) {
		throw new UnsupportedOperationException();
	}
}