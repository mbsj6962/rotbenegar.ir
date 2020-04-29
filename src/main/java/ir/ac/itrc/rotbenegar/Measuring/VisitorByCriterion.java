package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.DataFormats.CriterionType;
import ir.ac.itrc.rotbenegar.Utilities.TimeDate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.storage.StorageLevel;

public abstract class VisitorByCriterion implements java.io.Serializable {

    private Criterion criterion;
    protected Dataset<Row> emptyDF;

    public VisitorByCriterion(CriterionType criterionType) {
        switch (criterionType) {
            case BROWSER:
                criterion = new Browser();
                break;

            case COUNTRY:
                criterion = new Country();
                break;

            case OS:
                criterion = new OS();
                break;

            case PROVINCE:
                criterion = new Province();
                break;

            case REFERRER:
                criterion = new ReferrerType();
                break;

            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * records: JavaRDD<browser, os, referrerType, user_ip, user_ID>
     * Note: order of fields matters unless fields have column names
     */
    public Dataset<Row> persist(Dataset<Row> records, int logTypeID) {
        if (records == null) {
            return records;
        }

        records.persist(StorageLevel.MEMORY_AND_DISK());

        Dataset<Row> preparedRecoreds = prepareData(records);

        if (preparedRecoreds != null) {
            criterion.persist(preparedRecoreds, logTypeID);
        }

        return preparedRecoreds;
    }

    public Dataset<Row> persistAndReturn(Dataset<Row> records, int logTypeID) {
        if (records == null) {
            return records;
        }

        Dataset<Row> preparedRecoreds = prepareData(records);

        if (preparedRecoreds != null) {
            return criterion.persist(preparedRecoreds, logTypeID);
        }

        return preparedRecoreds;
    }

    /**
     * input: JavaRDD<browser, os, referrerType, user_ip, user_ID>
     * Note1: order of fields matters unless fields have column names
     *
     * output: JavaRDD<criterion_fld, user_ID_fld>
     * Note2: 'criterion_fld' represents either of browser, os, referrerType, or
     * user_ip fields.
     *
     * @param records
     * @return
     */
    abstract protected Dataset<Row> prepareData(Dataset<Row> records);
}
