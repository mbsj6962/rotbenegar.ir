package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.DataFormats.DatasetFactory;
import ir.ac.itrc.rotbenegar.DataFormats.Test;
import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import ir.ac.itrc.rotbenegar.Utilities.TimeDate;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import javax.swing.text.Utilities;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.callUDF;

public class Browser extends Criterion {

    private final String BROWSER_ID = "browserID";

    protected String getCriterionFilePath() {
        return DataFiles.getBrowserFilePath();
    }

    protected Dataset<Row> countDistinctUniqueVisitor(Dataset<Row> records) {
        Dataset<Row> dayDomainCriterionCount
                = records.groupBy(col(Measures.DataFieldsName.day.name()),
                        col(Measures.DataFieldsName.domain.name()),
                        col(Measures.DataFieldsName.browser.name()))
                        .agg(countDistinct(col(Measures.DataFieldsName.userID.name())).as(COUNT_FIELD));

        return dayDomainCriterionCount;
    }

    protected Dataset<Row> prepareCriterionID(Dataset<Row> records) {
        return records.withColumn(BROWSER_ID, callUDF("extractCriterionID", col(Measures.DataFieldsName.browser.name())));
    }

//    protected void saveIntoTables(Dataset<Row> records) {
    protected Dataset<Row> saveIntoTables(Dataset<Row> records) {
        DataFiles.saveNotFoundBrowser(
                records.select(col(Measures.DataFieldsName.browser.name()))
                        .where(col(BROWSER_ID).isNull())
        );

        Dataset<Row> retTable
                = records.select(col(LOG_TYPE_ID),
                        col(Measures.DataFieldsName.day.name()),
                        col(DOMAIN_ID),
                        col(BROWSER_ID),
                        col(COUNT_FIELD))
                        .where(col(BROWSER_ID).isNotNull())
                ;
        
        DataFiles.saveVisitorByBrowserData(
                retTable
        );
        
        return retTable;
    }

    public static void main(String[] args) throws IOException {
        Criterion br = new Browser();

        List<DatasetFactory.DataFields> data = Arrays.asList(
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12345", "1.1.1.1", "chrome", "winxp", "Link"),
                new DatasetFactory.DataFields(1453939200, "itrc.ac.ir", 1454000043, "12346", "1.1.1.1", "chrome", "winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "google.com", 1554000043, "12347", "1.1.1.1", "safari", "winxp", "Link"),
                new DatasetFactory.DataFields(1553939200, "itrc.ac.ir", 1554000043, "12348", "1.1.1.1", "android", "winxp", "Link")
        );

        Dataset<DatasetFactory.DataFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getDataFieldsEncoder());
        Dataset<Row> recordsToDF = records.toDF();

        br.persist(recordsToDF, 0);
//            br.prepareCriterionID(br.countDistinctUniqueVisitor(recordsToDF)).show();
//            br.saveIntoTables(new TimeDate(1553939200), recordsToDF, 0);
//        br.saveData(recordsToDF, 0);
    }

}
