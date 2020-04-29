package ir.ac.itrc.rotbenegar.Utilities;

import com.google.gson.Gson;
import ir.ac.itrc.rotbenegar.DataFormats.DatasetFactory;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.swing.text.Utilities;
import org.apache.spark.SparkFiles;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DataFiles {

    private static final String OUTPUT_FORMAT = "com.databricks.spark.csv";
    private static Boolean testMode = true;

    private static final String outputDir = "backend-out/";
    private static String dataDir;
    private static String outputPath;
    private static String inputPath;

    //Input log files
    private static String relativeInputFilePath;
    private static String inputFilePath;
    //end of input log files

    //Input files from "src/main/resources"
    private static String browserFilePath;
    private static String osFilePath;
    private static String referrerFilePath;
    private static String countryFilePath;
    private static String provinceFilePath;
    private static String cityFilePath;
    //end of input files from "src/main/resources"

    private static String fraudFileName;
    private static String fraudFilePath;

    private static String configFileName;
    private static String configFilePath;

    //Measuring output files
    private static String geoFileName;
    private static String geoFilePath;

    private static String notFoundBrowserPath;
    private static String notFoundOSPath;
    private static String notFoundReferrerPath;
    private static String notFoundCountryPath;
    private static String notFoundProvincePath;
    private static String notFoundCityPath;

    private static String visitorByBrowserPath;
    private static String visitorByOSPath;
    private static String visitorByReferrerPath;
    private static String visitorByCountryPath;
    private static String visitorByProvincePath;
    private static String visitorByCityPath;
    //end of Measuring output files

    //Pipeline output files
    private static String daily_Visit_Rank;
    private static String target_Site;
    //end of pipeline output files

    private static String targetSitePathTest;

    /**
     * static method
     */
//    public static String getInputPathApache(){
//    
//    return inputFileApache;
//    
//    }
    public static void setupMeasuringTests() {
        testMode = true;
        setDataPath("spark-warehouse/");
        setGeoFilePath("spark-warehouse/geo/GeoLiteCity.dat");
    }

    public static Configuration setup(String configPath) {
        try {
            DataFiles.setConfigFilePath(configPath);

            FileReader fileReader = new FileReader(SparkFiles.get(DataFiles.getConfigFileName()));
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            Gson gson = new Gson();
            Configuration fields = gson.fromJson(bufferedReader, Configuration.class);

            //set parameters
            //1.data path
            DataFiles.setDataPath(fields.getDataPath());
            //2.input data path
            if (fields.getRelativeInputDataPath() != null) {
                DataFiles.setRelativeInputFilePath(fields.getRelativeInputDataPath());
            }
            if (fields.getAbsoluteInputDataPath() != null) {
                DataFiles.setAbsoluteInputFilePath(fields.getAbsoluteInputDataPath());
            }
            //3.geo file path
            DataFiles.setGeoFilePath(fields.getGeoFilePath());
            //4.fraud file path
            DataFiles.setFraudFilePath(fields.getFraudFilePath());
            //5.test mode
            DataFiles.setTestMode(fields.getTestMode().toLowerCase().equals("true"));

            return fields;

        } catch (FileNotFoundException e) {
            DataFiles.reportExceptions(e);
            return null;
        }
    }

    private static String getCurrentDateTime() {
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss/");
        Date date = new Date();
        
        return sdf.format(date);
    }
    
    private static void setAllPaths() {
        outputPath = dataDir + outputDir + SparkHandlers.getJavaSparkContext().sc().applicationId() + "/"; getCurrentDateTime();
        inputPath = dataDir;

        //Input files from "src/main/resources"
        browserFilePath = "browser-Name_ID.txt";
        osFilePath = "os-Name_ID.txt";
        referrerFilePath = "referrer-Name_ID.txt";
        countryFilePath = "country-Name_ID.txt";
        provinceFilePath = "province-Name_ID.txt";
        cityFilePath = "city-Name_ID.txt";
        //end of input files from "src/main/resources"

        //Measuring output files
        notFoundBrowserPath = outputPath + "notFoundBrowser-Name";
        notFoundOSPath = outputPath + "notFoundOS-Name";
        notFoundReferrerPath = outputPath + "notFoundReferrer-Name";
        notFoundCountryPath = outputPath + "notFoundCountry-Name";
        notFoundProvincePath = outputPath + "notFoundProvince-Name";
        notFoundCityPath = outputPath + "notFoundCity-Name";

        visitorByBrowserPath = outputPath + "visitorByBrowser";
        visitorByOSPath = outputPath + "visitorByOS";
        visitorByReferrerPath = outputPath + "visitorByReferrer";
        visitorByCountryPath = outputPath + "visitorByCountry";
        visitorByProvincePath = outputPath + "visitorByProvince";
        visitorByCityPath = outputPath + "visitorByCity";
        //end of Measuring output files

        //Pipeline output files
        daily_Visit_Rank = outputPath + "dailyVisitRank";
        target_Site = outputPath + "targetSite";
        //end of pipeline output files

        targetSitePathTest = "targetSitePathTest";
    }

    public static void setDataPath(String dataPath) {
        dataDir = dataPath;
        if (dataDir.charAt(dataDir.length() - 1) != '/') {
            dataDir += "/";
        }

        setAllPaths();
    }

    public static String getConfigFileName() {
        return configFileName;
    }

    public static void setConfigFilePath(String configPath) {
        configFilePath = configPath;
        SparkHandlers.getJavaSparkContext().addFile(configFilePath);

        String[] configPathParts = configPath.split("/");
        configFileName = configPathParts[configPathParts.length - 1];
    }

    public static String getConfigFilePath() {
        return configFilePath;
    }

    public static void setRelativeInputFilePath(String relativePath) {
        relativeInputFilePath = relativePath;
        if (relativeInputFilePath.charAt(relativeInputFilePath.length() - 1) != '/' && relativeInputFilePath.charAt(relativeInputFilePath.length() - 1) != '*') {
            relativeInputFilePath += "/";
        }
        if (relativeInputFilePath.charAt(relativeInputFilePath.length() - 1) != '*') {
            relativeInputFilePath += "*";
        }

        inputFilePath = inputPath + relativeInputFilePath;
    }

    public static void setAbsoluteInputFilePath(String inputDataPath) {
        inputFilePath = inputDataPath;

        if (inputFilePath.charAt(inputFilePath.length() - 1) != '/' && inputFilePath.charAt(inputFilePath.length() - 1) != '*') {
            inputFilePath += "/";
        }
        if (inputFilePath.charAt(inputFilePath.length() - 1) != '*') {
            inputFilePath += "*";
        }
    }

    public static String getInputFilePath() {
        return inputFilePath;
    }
//    public static String getInputFilePathMCI() {
//        return inputFilePathMCI;
//    }
//
//    public static String getInputFilePathApache() {
//        return inputFilePathApache;
//    }

    public static void saveForDebugging(Dataset<Row> records, String filename) {
        records.write()
                .mode(SaveMode.Append)
                .format(OUTPUT_FORMAT)
                .option("header", "true")
                .save(outputPath + filename);
    }

    public static void saveHashMap(Map<String, Integer> idMap, String filename) {
        List<DatasetFactory.HashMapFields> data = new ArrayList<DatasetFactory.HashMapFields>();
        
        for (String name: idMap.keySet()){
            String key = name.toString();
            Integer value = idMap.get(name);  

            data.add(new DatasetFactory.HashMapFields (key, value));
        }
        
        Dataset<DatasetFactory.HashMapFields> records = SparkHandlers.getSparkSession().createDataset(data, DatasetFactory.getHashMapFieldsEncoder());
        Dataset<Row> recordsDF = records.toDF();

        recordsDF.write()
                .mode(SaveMode.Append)
                .format(OUTPUT_FORMAT)
                .option("header", "true")
                .save(outputPath + filename);
    }

    public static void reportExceptions(Exception e) {
        System.out.println("EXCEPTION: " + e.toString());
    }

    public static void saveNotFoundBrowser(Dataset<Row> records) {
        if (testMode) {
            return;
        }

        records.write()
                .mode(SaveMode.Append)
                .format(OUTPUT_FORMAT)
                .save(getNotFoundBrowserPath());
    }

    public static void saveNotFoundOS(Dataset<Row> records) {
        if (testMode) {
            return;
        }

        records.write()
                .mode(SaveMode.Append)
                .format(OUTPUT_FORMAT)
                .save(getNotFoundOSPath());
    }

    public static void saveNotFoundReferrer(Dataset<Row> records) {
        if (testMode) {
            return;
        }

        records.write()
                .mode(SaveMode.Append)
                .format(OUTPUT_FORMAT)
                .save(getNotFoundReferrerPath());
    }

    public static void saveNotFoundCountry(Dataset<Row> records) {
        if (testMode) {
            return;
        }

        records.write()
                .mode(SaveMode.Append)
                .format(OUTPUT_FORMAT)
                .save(getNotFoundCountryPath());
    }

    public static void saveNotFoundProvince(Dataset<Row> records) {
        if (testMode) {
            return;
        }

        records.write()
                .mode(SaveMode.Append)
                .format(OUTPUT_FORMAT)
                .save(getNotFoundProvincePath());
    }

    public static void saveNotFoundCity(Dataset<Row> records) {
        if (testMode) {
            return;
        }

        records.write()
                .mode(SaveMode.Append)
                .format(OUTPUT_FORMAT)
                .save(getNotFoundCityPath());
    }

    /**
     * static method
     *
     * @param records input:
     * records<browserid_fld, target_siteid_fld, log_type_id_fld, count_fld, date_fld>
     */
    public static void saveVisitorByBrowserData(Dataset<Row> records) {
        if (testMode) {
            return;
        }

        records.write()
                .mode(SaveMode.Append)
                .format(OUTPUT_FORMAT)
                .option("header", "true")
                .save(getVisitorByBrowserPath());
    }

    /**
     * static method
     *
     * @param records input:
     * records<os_id_fld, target_siteid_fld, log_type_id_fld, count_fld, date_fld>
     */
    public static void saveVisitorByOSData(Dataset<Row> records) {
        if (testMode) {
            return;
        }

        records.write()
                .mode(SaveMode.Append)
                .format(OUTPUT_FORMAT)
                .option("header", "true")
                .save(getVisitorByOSPath());
    }

    /**
     * static method
     *
     * @param records input:
     * records<target_siteid_fld, log_type_id_fld, country_id_fld, province_id_fld, city_id_fld, count_fld, date_fld>
     */
    public static void saveVisitorByLocationData(Dataset<Row> records) {
        if (testMode) {
            return;
        }

        throw new UnsupportedOperationException();
    }

    /**
     * static method
     *
     * @param parameter input:
     * records<referrertype_id_fld, target_siteid_fld, log_type_id_fld, count_fld, date_fld>
     */
    public static void saveVisitorByReferrerData(Dataset<Row> records) {
        if (testMode) {
            return;
        }

        records.write()
                .mode(SaveMode.Append)
                .format(OUTPUT_FORMAT)
                .save(getVisitorByReferrerPath());
    }

    public static void saveVisitorByCountryData(Dataset<Row> records) {
        if (testMode) {
            return;
        }

        records.write()
                .mode(SaveMode.Append)
                .format(OUTPUT_FORMAT)
                .save(getVisitorByCountryPath());
    }

    public static void saveVisitorByProvinceData(Dataset<Row> records) {
        if (testMode) {
            return;
        }

        records.write()
                .mode(SaveMode.Append)
                .format(OUTPUT_FORMAT)
                .save(getVisitorByProvincePath());
    }

    public static void saveVisitorByCityData(Dataset<Row> records) {
        if (testMode) {
            return;
        }

        records.write()
                .mode(SaveMode.Append)
                .format(OUTPUT_FORMAT)
                .save(getVisitorByCityPath());
    }

    /**
     * static method
     *
     * input:
     * JavaRDD<target_site_id_fld, log_id_fld, rank_formula_id_fld, rank_fld, scroe_fld, date_fld>
     */
    public static void saveRanks(Dataset<Row> records) {
        throw new UnsupportedOperationException();
    }

    public static void saveTargetSites(Dataset<Row> records) {
        throw new UnsupportedOperationException();
    }

    public static String getIP2DomainDataPath() {
        throw new UnsupportedOperationException();
    }

    public static void saveSiteVisit(Dataset<Row> records) {
        throw new UnsupportedOperationException();
    }

    public static void saveRankFormula(int rankFormulaID, int rankFormulaDescription) {
        throw new UnsupportedOperationException();
    }

//    public static void saveInvalidRecordsEqualZero(Dataset<Row> records) {
//
//        records.write().format(OUTPUT_FORMAT).option("header", "true").save(getInvalidRecordsEqualZero());
//
//    }
//
//    public static void saveInvalidRecordsNull(Dataset<Row> records) {
//
//        records.write().format(OUTPUT_FORMAT).option("header", "true").save(getInvalidRecordsNull());
//
//    }
//
//    public static void saveCleanRecords(Dataset<Row> records) {
//
//        records.write().format(OUTPUT_FORMAT).option("header", "true").save(getCleanRecords());
//
//    }
//
//    public static void saveNotCleanRecords(Dataset<Row> records) {
//
//        records.write().format(OUTPUT_FORMAT).option("header", "true").save(getNotCleanRecords());
//
//    }
//
//    public static void saveNullRecords(Dataset<Row> records) {
//
//        records.write().format(OUTPUT_FORMAT).option("header", "true").save(getNullRecords());
//
//    }
    public static void saveDailyVisitRank(Dataset<Row> records) {

        records.write().format(OUTPUT_FORMAT).option("header", "true").save(getDailyVisitRank());

    }

    public static void saveTargetSite(Dataset<Row> records) {

        records.write().format(OUTPUT_FORMAT).option("header", "true").save(getTargetSite());

    }

    public static String getBrowserFilePath() {
        return browserFilePath;
    }

    public static String getNotFoundBrowserPath() {
        return notFoundBrowserPath;
    }

    public static String getNotFoundOSPath() {
        return notFoundOSPath;
    }

    public static String getNotFoundReferrerPath() {
        return notFoundReferrerPath;
    }

    public static String getNotFoundCountryPath() {
        return notFoundCountryPath;
    }

    public static String getNotFoundProvincePath() {
        return notFoundProvincePath;
    }

    public static String getNotFoundCityPath() {
        return notFoundCityPath;
    }

    public static String getOSFilePath() {
        return osFilePath;
    }

    public static String getReferrerFilePath() {
        return referrerFilePath;
    }

    public static String getCityFilePath() {
        return cityFilePath;
    }

    public static String getProvinceFilePath() {
        return provinceFilePath;
    }

    public static String getCountryFilePath() {
        return countryFilePath;
    }

    public static String getLogTypeName() {
        throw new UnsupportedOperationException();
    }

    public static String getVisitorByBrowserPath() {
        return visitorByBrowserPath;
    }

    public static String getVisitorByOSPath() {
        return visitorByOSPath;
    }

    public static String getVisitorByReferrerPath() {
        return visitorByReferrerPath;
    }

    public static String getVisitorByCountryPath() {
        return visitorByCountryPath;
    }

    public static String getVisitorByProvincePath() {
        return visitorByProvincePath;
    }

    public static String getVisitorByCityPath() {
        return visitorByCityPath;
    }

    public static String getGeoFileName() {
        return geoFileName;
    }

    public static void setGeoFilePath(String filePath) {
        geoFilePath = filePath;

        String[] geoFilePathParts = geoFilePath.split("/");

        geoFileName = geoFilePathParts[geoFilePathParts.length - 1];
    }

    public static String getGeoFilePath() {
        return geoFilePath;
    }

    public static String getTargetSitePathTest() {

        return targetSitePathTest;

    }

//    public static String getInvalidRecordsEqualZero() {
//
//        return invalidRecordsEqualZero;
//
//    }
//
//    public static String getInvalidRecordsNull() {
//
//        return invalidRecordsNull;
//
//    }
//
//    public static String getCleanRecords() {
//
//        return cleanRecords;
//
//    }
//
//    public static String getNotCleanRecords() {
//
//        return NotCleanRecords;
//
//    }
//
//    public static String getNullRecords() {
//
//        return nullrecords;
//
//    }
    public static String getDailyVisitRank() {

        return daily_Visit_Rank;

    }

    public static String getTargetSite() {

        return target_Site;

    }

    public static String getFraudFileName() {
        return fraudFileName;
    }

    public static void setFraudFilePath(String fraudPath) {
        fraudFilePath = fraudPath;

        String[] fraudFilePathParts = fraudFilePath.split("/");

        fraudFileName = fraudFilePathParts[fraudFilePathParts.length - 1];
    }

    public static String getFraudFilePath() {
        return fraudFilePath;
    }

    public static void setTestMode(Boolean mode) {
        testMode = mode;
    }

    public static Boolean getTestMode() {
        return testMode;
    }
}
