package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import ir.ac.itrc.rotbenegar.Utilities.TimeDate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;

public class City extends Criterion {
        private final String CITY_ID = "cityID";

        protected String getCriterionFilePath() {
            return DataFiles.getCityFilePath();
        }

	protected Dataset<Row> countDistinctUniqueVisitor(Dataset<Row> records) {
            Dataset<Row> dayDomainCriterionCount = 
                    records.groupBy(col(Measures.DataFieldsName.day.name()), 
                                    col(Measures.DataFieldsName.domain.name()), 
                                    col(Measures.DataFieldsName.City.name()))
                           .agg(countDistinct(col(Measures.DataFieldsName.userID.name())).as(COUNT_FIELD));
            
            return dayDomainCriterionCount;
	}
 
        protected Dataset<Row> prepareCriterionID(Dataset<Row> records) {
            return records.withColumn(CITY_ID, callUDF("extractCriterionID", col(Measures.DataFieldsName.City.name())));
        }

//        protected void saveIntoTables(Dataset<Row> records) {
        protected Dataset<Row> saveIntoTables(Dataset<Row> records) {
            DataFiles.saveNotFoundCity(
                    records.select(col(Measures.DataFieldsName.City.name()))
                            .where(col(CITY_ID).isNull())
            );

            Dataset<Row> retTable =
                    records.select(col(LOG_TYPE_ID),
                            col(Measures.DataFieldsName.day.name()),
                            col(DOMAIN_ID),
                            col(CITY_ID),
                            col(COUNT_FIELD))
                            .where(col(CITY_ID).isNotNull())
                    ;
            
            DataFiles.saveVisitorByCityData(
                    retTable
            );
            
            return retTable;
        }
   
}