package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import ir.ac.itrc.rotbenegar.Utilities.TimeDate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;

public class ReferrerType extends Criterion {
        private final String REFERRER_ID = "referrerTypeID";

        protected String getCriterionFilePath() {
            return DataFiles.getReferrerFilePath();
        }

        protected Dataset<Row> countDistinctUniqueVisitor(Dataset<Row> records) {
            Dataset<Row> dayDomainCriterionCount
                    = records.groupBy(col(Measures.DataFieldsName.day.name()),
                            col(Measures.DataFieldsName.domain.name()),
                            col(Measures.DataFieldsName.ref.name()))
                            .agg(countDistinct(col(Measures.DataFieldsName.userID.name())).as(COUNT_FIELD));

            return dayDomainCriterionCount;
        }

        protected Dataset<Row> prepareCriterionID(Dataset<Row> records) {
            return records.withColumn(REFERRER_ID, callUDF("extractCriterionID", col(Measures.DataFieldsName.ref.name())));
        }

//        protected void saveIntoTables(Dataset<Row> records) {
        protected Dataset<Row> saveIntoTables(Dataset<Row> records) {
            DataFiles.saveNotFoundReferrer(
                    records.select(col(Measures.DataFieldsName.ref.name()))
                            .where(col(REFERRER_ID).isNull())
            );

            Dataset<Row> retTable = 
                    records.select(col(LOG_TYPE_ID),
                            col(Measures.DataFieldsName.day.name()),
                            col(DOMAIN_ID),
                            col(REFERRER_ID),
                            col(COUNT_FIELD))
                            .where(col(REFERRER_ID).isNotNull())
                    ;

            DataFiles.saveVisitorByReferrerData(
                    retTable
            );
            
            return retTable;
        }

}