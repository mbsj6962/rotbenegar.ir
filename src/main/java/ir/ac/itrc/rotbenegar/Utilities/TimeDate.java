package ir.ac.itrc.rotbenegar.Utilities;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.sql.Timestamp;
import java.util.Calendar;

public class TimeDate {
//	private Timestamp timestamp;
        private static Calendar cal = Calendar.getInstance();
        private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM/dd/yyyy");

//        public TimeDate (long timestamp) {
//            this.timestamp = new Timestamp(timestamp);
//        }
        
	public static String getDay(long timestamp) {
            Timestamp ts = new Timestamp(timestamp);
		return formatter.format((Date) ts);
	}
        
	public static String getDay2(long timestamp) {
            Timestamp ts = new Timestamp(timestamp);
            return simpleDateFormat.format(ts);
        }

        public static void main(String[] args) {
            
//            System.out.println(TimeDate.getDay2(1507447879));
 
            Timestamp stamp = new Timestamp(System.currentTimeMillis());
            System.out.println(stamp);
            
            Date date = new Date((long)1507447879*1000);//(stamp.getTime());
            System.out.println(date);
            
            System.out.println("current timestamp: "+System.currentTimeMillis()/1000);
        }
}
