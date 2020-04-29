package ir.ac.itrc.rotbenegar.Utilities;

import com.google.common.base.Charsets;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import com.google.common.hash.*;

public class IDMaker {

    private static Boolean configured = false;
    private static HashFunction hf = Hashing.md5();

//    static UDF1 createDomainID = new UDF1<String, String>() {
//        public String call(final String name) throws Exception {
//            return getTargetSiteID(name);
//        }
//    };
//
    public IDMaker() {
        if (!configured) {
//            SparkHandlers.getSQLContext().udf().register("createDomainID", createDomainID, DataTypes.StringType);
            configured = true;
        }
    }

    public static String getTargetSiteID(String targetSiteName) {
        HashCode hc = hf.newHasher()
                .putString(targetSiteName, Charsets.UTF_8)
                .hash();
        return hc.toString();
    }
    }
