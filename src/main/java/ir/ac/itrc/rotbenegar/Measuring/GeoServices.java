/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.DataFormats.CriterionType;
import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkFiles;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.broadcast.Broadcast;

public abstract class GeoServices extends VisitorByCriterion {

    protected Broadcast<LookupService> cl;
    private static Boolean configured = false;

//    UDF1 extractCountry = new UDF1<Seq<String>, String>() {
//        public String call(final Seq<String> userIP) throws Exception {
//            return userIP.apply(0);
//        }
//    };
//    UDF1 extractLocation = new UDF1<String, String[]>() {
//        public String[] call(final String userIP) throws Exception {
//            String[] a = new String[2];
//
//            a[0] = "C";
//            a[1] = "P";
//            return a;
//        }
//    };
    UDF1 extractLocation = new UDF1<String, String[]>() {
        public String[] call(final String userIP) throws Exception {
            String[] a = new String[2];

            if (userIP == null || userIP.trim().equals("")) {
                a = setGeoInfoAsNotFound(a);
            } else {
                Location location;
                try {
//                    location = cl.getLocation(userIP.trim());
                    location = cl.value().getLocation(userIP.trim());

                    try {
                        a[0] = location.countryCode.toLowerCase();

                        try {
                            a[1] = location.region.toLowerCase();
                        } catch (Exception e) {
                            a = setGeoInfoAsNotFound(a, 1);
                        }
                    } catch (Exception e) {
                        a = setGeoInfoAsNotFound(a);
                    }
                } catch (Exception e) {
                    a = setGeoInfoAsNotFound(a);
                }
            }

            return a;
        }
    };

    private static String[] setGeoInfoAsNotFound(String[] geoInfo) {
        geoInfo[0] = "";
        geoInfo[1] = "";

        return geoInfo;
    }

    private static String[] setGeoInfoAsNotFound(String[] geoInfo, int idx) {
        if (idx >= 0 && idx < geoInfo.length) {
            geoInfo[idx] = "";
        }

        return geoInfo;
    }

    public static LookupService getGeoData() {
        try {
            return new LookupService(SparkFiles.get(DataFiles.getGeoFileName()),
                    LookupService.GEOIP_MEMORY_CACHE | LookupService.GEOIP_CHECK_CACHE);
        } catch (Exception e) {
            configured = false;

            throw new UnsupportedOperationException();
        }
    }

    public GeoServices(CriterionType criterionType) {
        super(criterionType);

        if (!configured) {
            SparkHandlers.getSQLContext().udf().register("extractLocation", extractLocation, DataTypes.createArrayType(DataTypes.StringType));

            SparkHandlers.getJavaSparkContext().addFile(DataFiles.getGeoFilePath());
            
//            cl = getGeoData();
            cl = SparkHandlers.getJavaSparkContext().broadcast(getGeoData());

            configured = true;
        }
    }

    public Boolean countryColExists(String[] columns) {
        if (ArrayUtils.contains(columns, Measures.DataFieldsName.Country.name())) {
            return true;
        }
        return false;
    }

    public Boolean provinceColExists(String[] columns) {
        if (ArrayUtils.contains(columns, Measures.DataFieldsName.Province.name())) {
            return true;
        }
        return false;
    }
}
