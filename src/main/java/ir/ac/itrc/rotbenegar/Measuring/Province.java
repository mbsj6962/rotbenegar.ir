package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import ir.ac.itrc.rotbenegar.Utilities.TimeDate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;


public class Province extends Criterion {
        private static final String PROVINCE_ID = "provinceID";
        
        public static String getProvinceIDColName() {
            return PROVINCE_ID;
        }

        protected String getCriterionFilePath() {
            return DataFiles.getProvinceFilePath();
        }

        protected Dataset<Row> countDistinctUniqueVisitor(Dataset<Row> records) {
            Dataset<Row> dayDomainCriterionCount
                    = records.groupBy(col(Measures.DataFieldsName.day.name()),
                            col(Measures.DataFieldsName.domain.name()),
                            col(Measures.DataFieldsName.Province.name()))
                            .agg(countDistinct(col(Measures.DataFieldsName.userID.name())).as(COUNT_FIELD));

            return dayDomainCriterionCount;
        }

        protected Dataset<Row> prepareCriterionID(Dataset<Row> records) {
            return records.withColumn(PROVINCE_ID, callUDF("extractCriterionID", col(Measures.DataFieldsName.Province.name())));
        }

//        protected void saveIntoTables(Dataset<Row> records) {
        protected Dataset<Row> saveIntoTables(Dataset<Row> records) {
            DataFiles.saveNotFoundProvince(
                    records.select(col(Measures.DataFieldsName.Province.name()))
                            .where(col(PROVINCE_ID).isNull())
            );

            Dataset<Row> retTable = 
                    records.select(col(LOG_TYPE_ID),
                            col(Measures.DataFieldsName.day.name()),
                            col(DOMAIN_ID),
                            col(PROVINCE_ID),
                            col(COUNT_FIELD))
                            .where(col(PROVINCE_ID).isNotNull())
                    ;
            
            DataFiles.saveVisitorByProvinceData(
                    retTable
            );
            
            return retTable;
            
        }

}