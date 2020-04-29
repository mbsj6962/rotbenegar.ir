package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import ir.ac.itrc.rotbenegar.DataFormats.CriterionType;
import java.io.IOException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;
import org.apache.spark.sql.SparkSession;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

//import com.maxmind.geoip2.DatabaseReader;
//import com.maxmind.geoip2.exception.GeoIp2Exception;
//import com.maxmind.geoip2.model.CityResponse;
//import com.maxmind.geoip2.record.Country;

import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import java.io.File;
import java.net.InetAddress;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.util.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;
import org.apache.spark.storage.StorageLevel;

public class VisitorByCountry extends GeoServices {

    public VisitorByCountry() {
        super(CriterionType.COUNTRY);

    }

    /**
     * input: JavaRDD<browser, os, referrerType, user_ip, user_ID>
     * Note1: order of fields matters unless fields have column names
     *
     * output: JavaRDD<criterion_fld, user_ID_fld>
     * Note2: 'criterion_fld' represents either of browser, os, referrerType, or
     * user_ip fields.
     */
    protected Dataset<Row> prepareData(Dataset<Row> records) {
        String[] columns = records.columns();

        if (!ArrayUtils.contains(columns, Measures.DataFieldsName.userID.name())) {
            return null;
        }

        if (countryColExists(columns)) {
            return records;
        }

        if (ArrayUtils.contains(columns, Measures.DataFieldsName.userIP.name())) {
            Dataset<Row> recordsWithCountryProvince
                    = records.withColumn("location", callUDF("extractLocation", col(Measures.DataFieldsName.userIP.name())))
                            .withColumn(Measures.DataFieldsName.Country.name(), col("location").apply(0))
                            .withColumn(Measures.DataFieldsName.Province.name(), col("location").apply(1))
                            .drop("location");

            assert countryColExists(recordsWithCountryProvince.columns());

            recordsWithCountryProvince.persist(StorageLevel.MEMORY_AND_DISK());

//            recordsWithCountryProvince.printSchema();
//            recordsWithCountryProvince.show();
            recordsWithCountryProvince = recordsWithCountryProvince.withColumn("day", regexp_replace(col("day"), "-", ""));
            return recordsWithCountryProvince;
        }

        return null;
    }

//    public static void main(String[] args) throws IOException {
//        try {
//            // A File object pointing to your GeoIP2 or GeoLite2 database
//            File database = new File("/root/Downloads/geo/GeoLite2-City_20180102/GeoLite2-City.mmdb");
//
//            // This creates the DatabaseReader object. To improve performance, reuse
//            // the object across lookups. The object is thread-safe.
//            DatabaseReader reader = new DatabaseReader.Builder(database).build();
//
//            InetAddress ipAddress = InetAddress.getByName("128.101.101.101");
//
//            // Replace "city" with the appropriate method for your database, e.g.,
//            // "country".
//            CityResponse response = reader.city(ipAddress);
//
//            Country country = response.getCountry();
//            System.out.println(country.getIsoCode());            // 'US'
//            System.out.println(country.getName());               // 'United States'
//            System.out.println(country.getNames().get("zh-CN")); // '美国'
//        }
//        catch(Exception e){
//            
//        }
//    }
    
    
    
    public static void main(String[] args) throws IOException {
        LookupService cl = new LookupService("/root/Downloads/repo/projects/rotbenegar/spark-warehouse/geo/GeoLiteCity.dat",
                LookupService.GEOIP_MEMORY_CACHE | LookupService.GEOIP_CHECK_CACHE);

//        Location location = cl.getLocation("206.190.36.45"+"\t\t\t");
        Location location = cl.getLocation("192.206.151.131");
        System.out.println("Country:" + location.countryName + ", " + location.countryCode + ", Region:" + location.region);

        location = cl.getLocation("167.114.219.166");
        System.out.println("Country:" + location.countryName + ", " + location.countryCode + ", Region:" + location.region);

        location = cl.getLocation("206.45.68.182");
        System.out.println("Country:" + location.countryName + ", " + location.countryCode + ", Region:" + location.region);

//        String[] a = {"1", "2"};
//        a = test(a);
//        System.out.println("item0 = " + a[0]);
//        location = cl.getLocation("37.48.64.200");
//        System.out.println("Country:" + location.countryName + ", " + location.countryCode + ", Region:" + location.region);
//
//        location = cl.getLocation("188.158.130.221");
//        System.out.println("Country:" + location.countryName + ", " + location.countryCode + ", Region:" + location.region);
//        Dataset<Row> df = SparkHandlers.getSparkSession().read().json("/home/hduser/data/people.json");
//        df.show();
//
//        VisitorByCountry vbc = new VisitorByCountry();
//        Dataset<Row> results = vbc.prepareData(df);
//
//        if (results != null) {
//            results.show();
//        }
    }
}
