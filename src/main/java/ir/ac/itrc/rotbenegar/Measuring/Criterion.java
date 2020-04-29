package ir.ac.itrc.rotbenegar.Measuring;

import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import ir.ac.itrc.rotbenegar.Utilities.IDMaker;
import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import ir.ac.itrc.rotbenegar.Utilities.TimeDate;
import ir.ac.itrc.rotbenegar.Utilities.Logger;

import java.util.HashMap;
import java.util.Map;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;
import org.apache.spark.storage.StorageLevel;

public abstract class Criterion extends IDMaker implements java.io.Serializable {

    private Broadcast<Map<String, Integer>> criterionID;
//    private Map<String, Integer> idMap = new HashMap<String, Integer>();
    private static Boolean configured = false;

    protected final String COUNT_FIELD = "count";
    protected final String DOMAIN_ID = "domainID";
    protected final String LOG_TYPE_ID = "logTypeID";

    UDF1 extractCriterionID = new UDF1<String, Integer>() {
        public Integer call(final String name) throws Exception {

            Map<String, Integer> idMap = criterionID.value();
            Integer retVal = idMap.get(name);

            return retVal;//criterionID.value().get(name);
        }
    };
    
    static UDF1 createDomainID = new UDF1<String, String>() {
        public String call(final String name) throws Exception {
            return getTargetSiteID(name);
        }
    };

    public Criterion() {
        if (!configured) {
            SparkHandlers.getSQLContext().udf().register("extractCriterionID", extractCriterionID, DataTypes.IntegerType);
            SparkHandlers.getSQLContext().udf().register("createDomainID", createDomainID, DataTypes.StringType);

            configured = true;

            criterionID = loadID();
        }
    }

    private static BufferedReader readFileFromResources(String filename) throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream(filename);

        if (input == null) {
            String msg = filename + "NOT found!";

            Logger.write(msg);
            throw new IOException(msg);
        }

        return new BufferedReader(new InputStreamReader(input, "UTF-8"));
    }

    public static Broadcast<Map<String, Integer>> loadID() {
        ArrayList<String> myList = new ArrayList<String>();

        myList.add(DataFiles.getOSFilePath());
        myList.add(DataFiles.getReferrerFilePath());
        myList.add(DataFiles.getCountryFilePath());
        myList.add(DataFiles.getProvinceFilePath());
        myList.add(DataFiles.getBrowserFilePath());

        try {
            Map<String, Integer> idMap = new HashMap<String, Integer>();
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            
            for (int x = 0; x < myList.size(); x++) {
                InputStream input = classLoader.getResourceAsStream(myList.get(x));
                BufferedReader reader = new BufferedReader(new InputStreamReader(input, "UTF-8"));

                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.equals("")) {
                        continue;
                    }

                    String parts[] = line.split("\t");

                    assert parts.length == 2;

                    idMap.put(parts[0].trim().toLowerCase(), Integer.parseInt(parts[1].trim().toLowerCase()));
                }
                reader.close();
            }

            return SparkHandlers.getJavaSparkContext().broadcast(idMap);

        } catch (IOException e) {
            Logger.write("Criterion", e);

            return null;
        }

//        return null;
    }

    abstract protected String getCriterionFilePath();

//    public void persist(Dataset<Row> records, int logTypeID) {
    public Dataset<Row> persist(Dataset<Row> records, int logTypeID) {
        Dataset<Row> dayDomainCriterionCount = countDistinctUniqueVisitor(records);

        dayDomainCriterionCount.persist(StorageLevel.MEMORY_AND_DISK());
        return saveData(dayDomainCriterionCount, logTypeID);
    }

    abstract protected Dataset<Row> countDistinctUniqueVisitor(Dataset<Row> records);

    private Dataset<Row> prepareDomainID(Dataset<Row> records) {
        return records.withColumn(DOMAIN_ID, callUDF("createDomainID", col(Measures.DataFieldsName.domain.name())));
    }

    private Dataset<Row> prepareLogTypeID(Dataset<Row> records, int logTypeID) {
        return records.withColumn(LOG_TYPE_ID, lit(logTypeID));
    }

//    private void saveData(Dataset<Row> records, int logTypeID) {
    private Dataset<Row> saveData(Dataset<Row> records, int logTypeID) {
        Dataset<Row> recordsWithID = prepareCriterionID(prepareDomainID(prepareLogTypeID(records, logTypeID)));

//        recordsWithID.persist(StorageLevel.MEMORY_AND_DISK());
        return saveIntoTables(recordsWithID);
    }

    abstract protected Dataset<Row> prepareCriterionID(Dataset<Row> records);

//    abstract protected void saveIntoTables(Dataset<Row> records);
    abstract protected Dataset<Row> saveIntoTables(Dataset<Row> records);

}
