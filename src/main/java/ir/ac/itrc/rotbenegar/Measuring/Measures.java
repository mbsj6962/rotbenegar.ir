package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import ir.ac.itrc.rotbenegar.Utilities.TimeDate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class Measures {
	private VisitorByBrowser visitorByBrowser;
	private VisitorByOS visitorByOS;
	private VisitorByReferrer visitorByReferrer;
	private VisitorByProvince visitorByProvince;
	private VisitorByCountry visitorByCountry;
        
        public static enum DataFieldsName {
            browser, os, ref, userID, userIP, Country, Province, City,
            day, domain, criterion
        }

	public Measures() {
            visitorByBrowser = new VisitorByBrowser();
            visitorByCountry = new VisitorByCountry();
            visitorByOS = new VisitorByOS();
            visitorByProvince = new VisitorByProvince();
            visitorByReferrer = new VisitorByReferrer();
	}

	/**
	 * Note1:
	 * --'records' contains daily/domain data.
	 * --The parameter 'records' has only records for the day specified by the parameter 'day'.
	 * --All the rows in 'records' belong to a certain domain.
	 * 
	 * records: JavaRDD<browser, os, referrerType, user_ip, user_ID>
	 * Note2: order of fields matters unless fields have column names
         * @param day
         * @param records
         * @param logTypeID
	 */
	public void persistMeasures(Dataset<Row> records, int logTypeID) {
//            records = records.withColumnRenamed("user_ip", "userIP"); 
//            DataFiles.saveForDebugging(records, "Measures-persistMeasures");
            
            visitorByBrowser.persist(records, logTypeID);
            visitorByOS.persist(records, logTypeID);
            visitorByReferrer.persist(records, logTypeID);
            
            Dataset<Row> geoRecords = visitorByCountry.persist(records, logTypeID);
            visitorByProvince.persist(geoRecords, logTypeID);
	}
}
