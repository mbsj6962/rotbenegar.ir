package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.DataFormats.CriterionType;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;
import org.apache.spark.storage.StorageLevel;

public class VisitorByProvince extends GeoServices {

    public VisitorByProvince() {
        super(CriterionType.PROVINCE);
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

        if (provinceColExists(columns)) {
            return records;
        }

        if (ArrayUtils.contains(columns, Measures.DataFieldsName.userIP.name())) {
            Dataset<Row> recordsWithCountryProvince
                    = records.withColumn("location", callUDF("extractLocation", col(Measures.DataFieldsName.userIP.name())))
                            .withColumn(Measures.DataFieldsName.Country.name(), col("location").apply(0))
                            .withColumn(Measures.DataFieldsName.Province.name(), col("location").apply(1))
                            .drop("location");

            assert provinceColExists(recordsWithCountryProvince.columns());
            
            recordsWithCountryProvince.persist(StorageLevel.MEMORY_AND_DISK());
            
            recordsWithCountryProvince = recordsWithCountryProvince.withColumn("day", regexp_replace(col("day"), "-", ""));

            return recordsWithCountryProvince;
        }

        return null;
    }
}
