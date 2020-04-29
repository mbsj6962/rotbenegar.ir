package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import ir.ac.itrc.rotbenegar.Utilities.TimeDate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;

public class Country extends Criterion {
        private static final String COUNTRY_ID = "countryID";

        public static String getCountryIDColName() {
            return COUNTRY_ID;
        }
        
        protected String getCriterionFilePath() {
            return DataFiles.getCountryFilePath();
        }

        protected Dataset<Row> countDistinctUniqueVisitor(Dataset<Row> records) {
            Dataset<Row> dayDomainCriterionCount
                    = records.groupBy(col(Measures.DataFieldsName.day.name()),
                            col(Measures.DataFieldsName.domain.name()),
                            col(Measures.DataFieldsName.Country.name()))
                            .agg(countDistinct(col(Measures.DataFieldsName.userID.name())).as(COUNT_FIELD));

            return dayDomainCriterionCount;
        }

        protected Dataset<Row> prepareCriterionID(Dataset<Row> records) {
            return records.withColumn(COUNTRY_ID, callUDF("extractCriterionID", col(Measures.DataFieldsName.Country.name())));
        }

//        protected void saveIntoTables(Dataset<Row> records) {
        protected Dataset<Row> saveIntoTables(Dataset<Row> records) {
            DataFiles.saveNotFoundCountry(
                    records.select(col(Measures.DataFieldsName.Country.name()))
                            .where(col(COUNTRY_ID).isNull())
            );

            Dataset<Row> retTable =
                    records.select(col(LOG_TYPE_ID),
                            col(Measures.DataFieldsName.day.name()),
                            col(DOMAIN_ID),
                            col(COUNTRY_ID),
                            col(COUNT_FIELD))
                            .where(col(COUNTRY_ID).isNotNull())
                    ;

            DataFiles.saveVisitorByCountryData(
                    retTable
            );
            
            return retTable;

        }

}