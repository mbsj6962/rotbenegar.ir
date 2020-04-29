package ir.ac.itrc.rotbenegar.Pipeline;

import ir.ac.itrc.rotbenegar.DataFormats.DatasetFactory;
import ir.ac.itrc.rotbenegar.Utilities.Logger;
import ir.ac.itrc.rotbenegar.Utilities.UserAgent;
import ir.ac.itrc.rotbenegar.Utilities.Configuration;
import java.text.ParseException;
import static org.apache.spark.sql.functions.*;
import org.apache.commons.lang.StringUtils;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.io.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.spark_project.guava.net.InternetDomainName;

import ir.ac.itrc.rotbenegar.DataFormats.ReturnDataSets;
import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.glassfish.hk2.utilities.reflection.ReflectionHelper.cast;
import com.google.gson.Gson;
import org.apache.spark.SparkFiles;
import org.apache.spark.storage.StorageLevel;

public class Webranking {

    private RanksAndMeasures rankAndMeasures;
    private LogProcessing logProcessing;
    private String logTypeName;
    private String rankFormula;

    public Webranking(String configPath) {
        Configuration conf = DataFiles.setup(configPath);

        assert conf != null;

        logTypeName = conf.getLogType();
        rankFormula = conf.getRankFormula();

//        System.out.println("***************" + logTypeName);
//        System.out.println("***************" + rankFormula);
//        System.out.println("***************" + DataFiles.getConfigFileName());
//        System.out.println("***************" + DataFiles.getConfigFilePath());
//        System.out.println("***************" + conf.getDataPath());
//        System.out.println("***************" + DataFiles.getGeoFileName());
//        System.out.println("***************" + DataFiles.getGeoFilePath());
//        System.out.println("***************" + DataFiles.getFraudFileName());
//        System.out.println("***************" + DataFiles.getFraudFilePath());
//        System.out.println("***************" + DataFiles.getTestMode());
//        
//        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Rotbenegar Execution Start !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

        if (args.length != 1) {
            System.out.println("Error: the only input argument must be the path of the configurations file.");
            System.exit(0);
        }

        Webranking wr = new Webranking(args[0]);

        String path = DataFiles.getInputFilePath();

        Dataset<Row> newds = wr.doETL(path);

        // fraud detection
        Fraud fraud = new Fraud();
        fraud.fraudDetection(newds);

        wr.persist(newds, wr.getRankFormula()); // "LogRank" is the forumla Name

        SparkHandlers.Stop();
    }

    public String getLogTypeName() {
        return logTypeName;
    }

    public String getRankFormula() {
        return rankFormula;
    }

    public Dataset<Row> doETL(String path) {
        logProcessing = new LogProcessing(logTypeName);
        Dataset<Row> result;
//        System.out.println("logTypeName : " + logTypeName);
        if (logProcessing.isApacheLog(logTypeName)) {

            result = logProcessing.preprocessApache(path);
        } else {

            result = logProcessing.preprocess(path);

        }

        return result;

    }

    public void persist(Dataset<Row> records, String rankFormulaDescription) throws Exception {
        records.persist(StorageLevel.MEMORY_AND_DISK());

        rankAndMeasures = new RanksAndMeasures();

        rankAndMeasures.persistRank(records, rankFormulaDescription,
                logProcessing.getLogTypeID()
        );
        records.persist(StorageLevel.MEMORY_AND_DISK());

        rankAndMeasures.persistMeasures(records,
                logProcessing.getLogTypeID()
        );
    }
}
