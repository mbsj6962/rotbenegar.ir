package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import ir.ac.itrc.rotbenegar.Utilities.TimeDate;
import java.io.IOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;

public class OS extends Criterion {
    
        private final String OS_ID = "osID";

        protected String getCriterionFilePath() {
            return DataFiles.getOSFilePath();
        }
    
    	protected Dataset<Row> countDistinctUniqueVisitor(Dataset<Row> records) {
            Dataset<Row> dayDomainCriterionCount = 
                    records.groupBy(col(Measures.DataFieldsName.day.name()), 
                                    col(Measures.DataFieldsName.domain.name()), 
                                    col(Measures.DataFieldsName.os.name()))
                           .agg(countDistinct(col(Measures.DataFieldsName.userID.name())).as(COUNT_FIELD));
            
            return dayDomainCriterionCount;
	}

        protected Dataset<Row> prepareCriterionID(Dataset<Row> records) {
            return records.withColumn(OS_ID, callUDF("extractCriterionID", col(Measures.DataFieldsName.os.name())));
        }

//        protected void saveIntoTables(Dataset<Row> records) {
        protected Dataset<Row> saveIntoTables(Dataset<Row> records) {
            DataFiles.saveNotFoundOS(
                    records.select(col(Measures.DataFieldsName.os.name()))
                            .where(col(OS_ID).isNull())
            );

            Dataset<Row> retTable =
                    records.select(col(LOG_TYPE_ID),
                            col(Measures.DataFieldsName.day.name()),
                            col(DOMAIN_ID),
                            col(OS_ID),
                            col(COUNT_FIELD))
                            .where(col(OS_ID).isNotNull())
                    ;

            DataFiles.saveVisitorByOSData(
                    retTable
            );
            
            return retTable;

        }

        public static void main(String[] args) throws IOException {
            Criterion os = new OS();

            System.out.println(os.getCriterionFilePath());
        }    
}