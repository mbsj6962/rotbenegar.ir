package ir.ac.itrc.rotbenegar.Pipeline;
//
//

import ir.ac.itrc.rotbenegar.Ranking.LogRank;
//import ir.ac.itrc.rotbenegar.Ranking.LogRank1;
import ir.ac.itrc.rotbenegar.Ranking.Score;
import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import ir.ac.itrc.rotbenegar.Utilities.TimeFormat;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import org.apache.spark.sql.api.java.UDF1;
import ir.ac.itrc.rotbenegar.Pipeline.RanksAndMeasures;
import ir.ac.itrc.rotbenegar.Ranking.HostExtract;
import ir.ac.itrc.rotbenegar.Utilities.DataFiles;
import ir.ac.itrc.rotbenegar.Utilities.Logger;
import ir.ac.itrc.rotbenegar.Utilities.UserAgent;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.spark_project.guava.net.InternetDomainName;

/**
 * output : JavaRDD<records>
 */
public class LogProcessing implements java.io.Serializable {

    /**
     * dd JavaRDD<string>
     */
    private String typeName;
    public RanksAndMeasures unnamed_RanksAndMeasures_;
    public Webranking unnamed_Webranking_;
    public TimeFormat unnamed_TimeFormat_;
    String exDomain;
    String TimeStamp = "TimeStamp";
    String domain = "domain";
    String userID = "userID";
    String user_ip = "userIP";
    String day = "day";
    String os = "os";
    String browser = "browser";
    String ref = "ref";
    String Events = "Events";
    String targetHost = "targetHost";
    String user_Agent = "user_Agent";
    String uri = "uri";

    public LogProcessing(String logTypeName) {
        typeName = logTypeName;
    }

    public boolean isApacheLog(String logTypeName) {
        return (logTypeName.equals("Apache")) ? true : false;
    }

    private boolean isMCILog(String logTypeName) {
        return (logTypeName.equals("MCI")) ? true : false;
    }

    /**
     * output Apache:
     * JavaRDD<timestamp, user-ID , user_ip , os , browser, domain_name, refferer>
     * MCI: JavaRDD<timestamp, user-ID , user_ip ,domain_name>
     */
    public Dataset<Row> preprocess(String path) {
        Dataset<Row> df = read(path);
        df = parse(df);
        df = clean(df);
        return df;
    }

    public Dataset<Row> preprocessApache(String path) {

        Dataset<Row> df = readApache(path);
        df = parseApache(df);
        df = cleanApache(df);

//        DataFiles.saveCleanRecords(df);
//        df.show();
//        System.out.println("123123123123123");
        return df;
    }

    /**
     * output: JavaRDD<records>
     */
    private Dataset<Row> readApache(String path) {

        Dataset<Row> inputFile = SparkHandlers.getSparkSession().read()
                .option("delimiter", "\t")
                .text(path)
                .withColumnRenamed("value", Events)
                .filter(col(Events).contains("GET /script.php"));
//        inputFile.show(false);
        return inputFile;
    }

    private Dataset<Row> parseApache(Dataset<Row> inputFile) {

//        SparkSession spark = SparkHandlers.getSparkSession();
        UDF1 userIpUDf = new UDF1< String, String>() {

            public String call(final String Events) throws Exception {
                String[] UserIp = Events.split(" ");

                String userIp = UserIp[0];

                return userIp;
            }
        };

        SparkHandlers.getSparkSession().udf().register("userIpUDf", userIpUDf, DataTypes.StringType);
        inputFile = inputFile.withColumn(user_ip, callUDF("userIpUDf", col(Events)));
//        inputFile.show(false);
        UDF1 StartTimeUDF;
        StartTimeUDF = new UDF1<String, String>() {

            public String call(final String Events) throws Exception {

                String[] StartTimeTokensFirst = Events.split("] ");

                String BeforeCrusheMark = StartTimeTokensFirst[0];

                String[] StartTimeTokensEnd = BeforeCrusheMark.split(" - - \\[");

                String startTime = StartTimeTokensEnd[1];

                String dateToken = StringUtils.substringBefore(startTime, ":");

                String x = dateToken.replaceAll("/", "-");

                String month = StringUtils.substringBeforeLast(x, "-");

                String month1 = StringUtils.substringAfter(month, "-");

                org.joda.time.format.DateTimeFormatter format = DateTimeFormat.forPattern("MMM");
                DateTime instance = format.withLocale(Locale.ENGLISH).parseDateTime(month1);

                int month_number = instance.getMonthOfYear();

                String xx = Integer.toString(month_number);

                String finalDate = x
                        .replaceAll("(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|(Nov|Dec)(?:ember)?)", xx);

                SimpleDateFormat originalFormat = new SimpleDateFormat("dd-MM-yyyy");
                SimpleDateFormat targetFormat = new SimpleDateFormat("yyyy-MM-dd");
                Date date = null;
                try {
                    date = originalFormat.parse(finalDate);

                } catch (ParseException ex) {
                    throw new UnsupportedOperationException();
                }
                return targetFormat.format(date);
            }

        };

        SparkHandlers.getSparkSession().udf().register("StartTimeUDF", StartTimeUDF, DataTypes.StringType);
        inputFile = inputFile.withColumn(day, callUDF("StartTimeUDF", col(Events)));
//        inputFile.show(false);
//        inputFile.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/hhduser/ResultEndToEnd/starttime");
        UDF1 urlUDF;
        urlUDF = new UDF1<String, String>() {

            public String call(final String Events) throws Exception {
                String result = null;
                try {
                    String[] TargetHostToken = Events.split("&");

                    String targeHost = TargetHostToken[0];

                    String[] TargetHostMainToken = targeHost.split("\\?url=");
//                System.out.println("XxX " + TargetHostMainToken[1]);

                    String targetHostMain = TargetHostMainToken[1];
                    result = java.net.URLDecoder.decode(targetHostMain, "UTF-8");
                } catch (Exception e) {

                }

                return result;
            }

        };

        SparkHandlers.getSparkSession().udf().register("urlUDF", urlUDF, DataTypes.StringType);
////        countNum = 1 + countNum;
        inputFile = inputFile.withColumn(targetHost, callUDF("urlUDF", col(Events)));
//        inputFile.show(false);
//        inputFile.show(1000);

//        inputFile = inputFile.select(col("targetHost"));
//        inputFile.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/hhduser/ResultEndToEnd/targetHost");
        UDF1 referrerUDF = new UDF1<String, String>() {

            public String call(final String Events) throws Exception {
                String[] searchengine = {"google", "yahoo", "bing", "ask", "baidu", "parsijoo"};

                String refererFinal = null;
                try {
                    String[] refererrMainToken = Events.split("&rnd=");

                    String refererMain = refererrMainToken[0];

                    String[] refererToken = refererMain.split("&referrer");

                    String referer = refererToken[1];

                    String result = java.net.URLDecoder.decode(referer, "UTF-8");
                    refererFinal = result.substring(1, result.length());

                    if (refererFinal.isEmpty()) {
                        refererFinal = "Direct";
                    } else if (ArrayUtils.contains(searchengine, refererFinal.toLowerCase())) {
                        refererFinal = "SE";
                    } else {
                        refererFinal = "Link";
                    }

                } catch (Exception e) {

                }

                return refererFinal.toLowerCase();
            }

        };

        SparkHandlers.getSparkSession().udf().register("referrerUDF", referrerUDF, DataTypes.StringType);
        inputFile = inputFile.withColumn(ref, callUDF("referrerUDF", col(Events)));
//        System.out.println("1q2w3e");
//        inputFile.show();
//        inputFile.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/hhduser/ResultEndToEnd/referer");
////        inputFile = inputFile.select("referer");
        UDF1 user_agentUDF = (UDF1<String, String>) (final String Events) -> {
            String userAgent1 = null;
            try {
                String[] domainMainToken1 = Events.split("&t=l");

                String domainMain1 = domainMainToken1[1];
                String[] userAgentToken1 = domainMain1.split("\" \"");
                userAgent1 = userAgentToken1[1].substring(0, userAgentToken1[1].length() - 1);
//            System.out.println("countNum = " + countNum);
            } catch (Exception e) {

            }

            return userAgent1;
        };

        SparkHandlers.getSparkSession().udf().register("user_agentUDF", user_agentUDF, DataTypes.StringType);
        inputFile = inputFile.withColumn(user_Agent, callUDF("user_agentUDF", col(Events)));
//        inputFile.show(false);
////        inputFile.select("user_Agent").printSchema();
////       inputFile = inputFile.drop(col("Events"));
////        inputFile.printSchema();
//        inputFile.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/hhduser/ResultEndToEnd/user_Agent");
////
        UDF1 uriUDF = new UDF1<String, String>() {

            public String call(final String target_host) throws Exception {
                String t = null;
                try {
                    String delimiter = "/";
                    int count = 3;

                    t = target_host.replaceAll("^(.*?" + delimiter + "){" + count + "}(.*)", "$2");

                } catch (Exception e) {

                }

                return t;
            }

        };

        SparkHandlers.getSparkSession().udf().register("uriUDF", uriUDF, DataTypes.StringType);
        inputFile = inputFile.withColumn(uri, callUDF("uriUDF", col(targetHost)));
//        inputFile.show(false);
        UDF1 domainUdf = new UDF1< String, String>() {
            String result157 = null;

            public String call(final String Events) throws Exception {
                String destination = null;
                String result123123 = null;
                try {

                    destination = Events.split("&sessionId=")[0].split("%3A%2F%2F")[1].split("%2F")[0];
                    result123123 = java.net.URLDecoder.decode(destination, "UTF-8").toLowerCase();
                    result123123 = InternetDomainName.from(result123123).topPrivateDomain().name().trim();

                } catch (Exception e) {
                }
                return result123123;

            }
        };

        SparkHandlers.getSparkSession().udf().register("domainUdf", domainUdf, DataTypes.StringType);
        inputFile = inputFile.withColumn(domain, callUDF("domainUdf", col(Events)));
//        inputFile.show(false);
//        System.out.println("123456123456");
        UDF1 BrowserUDF = new UDF1<String, String>() {

            public String call(final String user_agent) throws Exception {
                String browsername = null;
                try {
                    browsername = UserAgent.parseUserAgentString(user_agent).getBrowser().toString().replaceAll("[^A-Za-z]", "").toLowerCase();
                } catch (Exception e) {

                }

                return browsername;
            }

        };

        SparkHandlers.getSparkSession().udf().register("BrowserUDF", BrowserUDF, DataTypes.StringType);
        inputFile = inputFile.withColumn(browser, callUDF("BrowserUDF", col(user_Agent)));
//        inputFile.createOrReplaceGlobalTempView("inputFile");
//        inputFile = SparkHandlers.getSQLContext().sql("UPDATE inputFile SET browser = OTHERBROWSER WHERE browser = unknown");
//        inputFile.select(col(browser)).show(100000,false);
        UDF1 OSUDF = new UDF1<String, String>() {

            public String call(final String user_agent) throws Exception {
                String osname = null;
//                String trimOS = null;
                try {
                    osname = UserAgent.parseUserAgentString(user_agent).getOperatingSystem().toString().toLowerCase();

                } catch (Exception e) {

                }

                return osname;
            }

        };

        SparkHandlers.getSparkSession().udf().register("OSUDF", OSUDF, DataTypes.StringType);
        inputFile = inputFile.withColumn(os, callUDF("OSUDF", col(user_Agent)))
                .drop(col(targetHost)).drop(col(user_Agent));
//        inputFile.show(false);
//        System.out.println("12321323131231231");
//        inputFile = inputFile.drop("Events");
//        inputFile.coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/hhduser/logresult/OperatingSystem");

//        inputFile = inputFile.withColumn("xyz", regexp_extract(col("Events"), "regex", 0));
        Dataset<Row> df2 = inputFile.withColumn(day, inputFile.col(day).cast("date"));

        return df2;
    }

    private Dataset<Row> cleanApache(Dataset<Row> df2) {

        df2 = df2.filter(col(day).isNotNull())
                .filter(col(user_ip).isNotNull())
                //                .filter(col("uri").isNotNull())
                //                .filter(col("session_indicator").isNotNull())
                //                .filter(col("visitor_indicator").isNotNull())
                //                .filter(col("requested_page_title").isNotNull())
                .filter(col(domain).isNotNull());

        df2 = df2.withColumn(TimeStamp, df2.col(day).cast(TimestampType).cast(IntegerType))
                .withColumn(userID, concat(df2.col(user_ip), lit("|"), df2.col(os), lit("|"), df2.col(browser)));
//        df2.show(false);
        df2 = df2.select(col(TimeStamp), col(domain), col(userID), col(user_ip), col(day), col(os), col(browser), col(ref));

//        df2.show(false);
//        System.out.println("count second is : " + df2.count());
        return df2;
    }

    private Dataset<Row> read(String path) {
        //read csv file
        Dataset<Row> df = SparkHandlers.getSparkSession().read()
                .option("header", true)
                .option("delimiter", "\t")
                .csv(path);
//        System.out.println(" mohamad basij  = = = = " + df.count());

//df.show();
//        df = df.filter(col("start_time").isNotNull()).filter(col("user_ip").isNotNull()).filter(col("dst_ip").isNotNull())
//                .filter(col("host").isNotNull());
        // change column dataType for casting
        return df;
    }

    private Dataset<Row> selectFields(Dataset<Row> records) {

        records = records.select(col("start_time"), col("user_ip"), col("dst_ip"), col("host"));
        return records;
    }

    private Dataset<Row> castingFields(Dataset<Row> records) {

        records = records.select(col("start_time"), col("user_ip"), col("dst_ip"), col("host"));
        records = records.withColumnRenamed("start_time", "TimeStamp")
                .withColumnRenamed("dst_ip", "destination_ip")
                .withColumnRenamed("host", "destination_host");
//        records.show();
        return records;
    }

    private Dataset<Row> castingTimeStampToLong(Dataset<Row> records) {
        return records.withColumn("TimeStamp", records.col("TimeStamp").cast(TimestampType).cast(IntegerType));
    }

    /**
     * input Apache :
     * JavaRDD<request-time, user_ip , os , browser, destination[ip|host], refferer>
     * MCI : JavaRDD<request-time, user_ip , destination_ip, destination_host>
     *
     * output : Apache :
     * JavaRDD<TimeStamp, user_ID, user_ip , os , browser, destination[ip|host], refferer>
     * MCI :
     * JavaRDD<TimeStamp, user_ID, user_ip , destination_ip, destination_host>
     *
     * @param records JavaRDD<Tuple>
     */
    private Dataset<Row> parse(Dataset<Row> records) {
//        TODO: if log == MCI do parse1
//        else if log == Apache do parse2

        // MCI log header
        // start_time,user_ip,dst_ip,host,up_bytes,down_bytes,user_agent,uri,content_type,referer
        if (records.columns()[0].contains("start_time") && records.columns()[1].contains("user_ip")
                && records.columns()[2].contains("dst_ip") && records.columns()[3].contains("host")
                && records.columns()[4].contains("up_bytes") && records.columns()[5].contains("down_bytes")
                && records.columns()[6].contains("user_agent") && records.columns()[7].contains("uri")
                && records.columns()[8].contains("content_type") && records.columns()[9].contains("referer")) {
            records = selectFields(records);
            records = castingFields(records);

//        records = records.withColumn("user_ID", concat(col("user_ip"),lit("|"),col("dest_ip"),lit("|"),col("host"))); //records.col("user_port").c);
            // day calculation
            records = records.withColumn("user_ID", concat(col("user_ip"))).
                    withColumn("day", functions.lit(records.col("TimeStamp").cast(DateType))); //records.col("user_port").c);
            records = castingTimeStampToLong(records);
        } else {
            // TODO : Apache do parse2
        }
        return records;
    }

    /**
     * input: Apache :
     * JavaRDD<TimeStamp, user_ID, user_ip , os , browser, destination[ip|host], refferer>
     * MCI :
     * JavaRDD<TimeStamp, user_ID, user_ip , destination_ip, destination_host>
     *
     * output: Apache :
     * JavaRDD<TimeStamp, user_ID, user_ip , os , browser, domain_name, refferer>
     * MCI : JavaRDD<TimeStamp, user_ID, user_ip , domain_name>
     *
     * @param records JavaRDD<Tuple>
     */
//    public String function(String dest) {
//
//        dest = dest.replaceAll(":(\\d{1,5})", "");
//
//        return dest;
//    }
    public Dataset<Row> clean(Dataset<Row> records) {

        boolean ismciLog = isMCILog(typeName);
        String regex = "^(http:\\/\\/www\\.|https:\\/\\/www\\.|http:\\/\\/|https:\\/\\/)?[a-z0-9]+([\\-\\.]{1}[a-z0-9]+)*\\.[a-z]{2,5}(:[0-9]{1,5})?(\\/.*)?:(\\d{1,5})$";
        String regexTLDPort = "[.]?.*[.x][a-z]{2,3}:(\\d{1,5})";
        Dataset<Row> notNullFields = records.filter(col("TimeStamp").isNotNull())
                .filter(col("user_ip").isNotNull()).filter(col("user_ID").isNotNull())
                .filter(col("destination_host").isNotNull()
                        .or(col("destination_host").rlike("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):(\\d{1,5})"))
                        .or(col("destination_host").rlike("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})"))
                        .or(col("destination_host").rlike(regex))
                        .or(col("destination_host").rlike("/^[a-zA-Z0-9][a-zA-Z0-9-]{1,61}[a-zA-Z0-9]\\.[a-zA-Z]{2,}$/"))
                ).distinct();

        Dataset<Row> nullFields = records.filter(col("TimeStamp").isNull()).filter(col("user_ip").isNull()).filter(col("user_ID").isNull());

        if (ismciLog) {

            UDF1 tld = new UDF1< String, String>() {

                public String call(final String destination_host) throws Exception {
                    exDomain = destination_host;
//
                    try {
                        exDomain = InternetDomainName.from(destination_host).topPrivateDomain().name().trim();

                    } catch (Exception e) {
//                        Logger.write("Not Valid Domain for " + destination_host);
                    }

                    return exDomain;

                }
            };

            SparkSession spark = SparkHandlers.getSparkSession();
            spark.udf().register("tld", tld, DataTypes.StringType);
            notNullFields = notNullFields.withColumn("destination", callUDF("tld", col("destination_host"))
            );

            UDF1 removePort = new UDF1< String, String>() {

                public String call(final String destination_host) throws Exception {
                    exDomain = destination_host;
                    try {
                        if (exDomain.matches(regexTLDPort) || exDomain.matches(regex)) {

                            exDomain = exDomain.replaceAll(":(\\d{1,5})", "");

                        }
                    } catch (Exception e) {
//                        Logger.write("Not Valid Domain for " + destination_host);
                    }

                    return exDomain;

                }
            };

            spark.udf().register("removePort", removePort, DataTypes.StringType);
            notNullFields = notNullFields.withColumn("destination", callUDF("removePort", col("destination")));

//            notNullFields = notNullFields.selectExpr("*",
//                    "split(destination, '[.]?.*[.x][a-z]{2,3}') as port"
//                    ,"split(destination, ':(\\d{1,5})') as domain"
//);
//            Dataset<Row> domainPort = notNullFields.filter(col("destination").rlike("[.]?.*[.x][a-z]{2,3}:(\\d{1,5})"));

            UDF1 host1 = new UDF1< String, String>() {

//                final long startTime = System.currentTimeMillis();
                public String call(final String destination_host) throws Exception {
                    exDomain = destination_host;

                    try {
//                        exDomain = exDomain.replaceAll(":(\\d{1,5})", "").trim();
                        exDomain = InternetDomainName.from(destination_host).topPrivateDomain().name();

//                        exDomain = InternetDomainName.from(destination_host).topPrivateDomain().name().trim();
//                        exDomain=exDomain.replaceAll(":(\\d{1,5})", "");
//                        exDomain=destination_host.replace("www.google.com", "Telegram");
                    } catch (Exception e) {
//                        Logger.write("Not Valid Domain for " + destination_host);
                    }

                    return exDomain;

                }
            };

//            SparkSession spark = SparkHandlers.getSparkSession();
            spark.udf().register("host1", host1, DataTypes.StringType);
            notNullFields = notNullFields.withColumn("destination", callUDF("host1", col("destination")));

//            domainPort.show(false);
//            System.out.println("finalDataset===123123");
//            UDF1 host2 = new UDF1< String, String>() {
//
////                final long startTime = System.currentTimeMillis();
//                public String call(final String destination_host) throws Exception {
//                    exDomain = destination_host;
//
//                    try {
////                        exDomain = exDomain.replaceAll(":(\\d{1,5})", "");
//                        exDomain = InternetDomainName.from(destination_host).topPrivateDomain().name().trim();
//
////                        exDomain = InternetDomainName.from(destination_host).topPrivateDomain().name().trim();
////                        exDomain=exDomain.replaceAll(":(\\d{1,5})", "");
////                        exDomain=destination_host.replace("www.google.com", "Telegram");
//                    } catch (Exception e) {
////                        Logger.write("Not Valid Domain for " + destination_host);
//                    }
//
//                    return exDomain;
//
//                }
//            };
//
////            SparkSession spark = SparkHandlers.getSparkSession();
//            spark.udf().register("host2", host2, DataTypes.StringType);
//            domainPort = domainPort.withColumn("destination", callUDF("host2", col("destination")));

//            domainPort.show(false);
//            System.out.println("domainport");
//            notNullFields.show(false);
//            System.out.println("domainport1");
//            notNullFields.select(col("destination_host")).coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/hhduser/destination_host");;
//            notNullFields.select(col("destination")).coalesce(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/hhduser/destination");;
//            notNullFields =notNullFields.rdd().map(function, evidence$3);
//            notNullFields.show(false);
//            System.out.println("finalDataset===");
//            UDF1 host1 = new UDF1< String, String>() {
//
////                final long startTime = System.currentTimeMillis();
//
//                public String call(final String destination_host) throws Exception {
//                    exDomain = destination_host;
//
//                    try {
////                        if (exDomain.matches("[.]?.*[.x][a-z]{2,3}:(\\d{1,5}))")) {
//                            exDomain = exDomain.replaceAll(":(\\d{1,5}))", "");
////                        }
////                        exDomain = InternetDomainName.from(destination_host).topPrivateDomain().name().trim();
////                        exDomain=exDomain.replaceAll(":(\\d{1,5})", "");
////                        exDomain=destination_host.replace("www.google.com", "Telegram");
//                    } catch (Exception e) {
////                        Logger.write("Not Valid Domain for " + destination_host);
//                    }
//
//                    return exDomain;
//
//                }
//            };
//
////            SparkSession spark = SparkHandlers.getSparkSession();
//            spark.udf().register("host1", host1, DataTypes.StringType);
//            notNullFields = notNullFields.withColumn("destination1", callUDF("host1", col("destination")));
//
//            notNullFields.show(false);
//            System.out.println("finalDataset===123");
        }
//
//        notNullFields = notNullFields.filter(col("destination").rlike("[.]?.*[.x][a-z]{2,3}:(\\d{1,5})")
//                .or(col("destination").rlike("[.]?.*[.x][a-z]{2,3}"))
//                .or(col("destination").rlike("(\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}):(\\\\d{1,5})"))
//                .or(col("destination").rlike("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})")));
//        notNullFields.show(false);
//        System.out.println("finalDataset===111");
//
//        UDF1 host1 = new UDF1< String, String>() {
//
//            public String call(String destination_host) throws Exception {
//
//                try {
//                    if (destination_host.matches("[.]?.*[.x][a-z]{2,3}:(\\d{1,5})")) {
//
//                        destination_host = destination_host.replaceAll(":(\\d{1,5})", "");
////                        Logger.write("Not Valid Domain for " + destination_host);
//                    }
//                } catch (UnsupportedOperationException e) {
//
//                }
//
//                return destination_host;
//            }
//
//        };
//        SparkHandlers.getSparkSession().udf().register("host1", host1, DataTypes.StringType);
//        notNullFields = notNullFields.withColumn("destination1", callUDF("host1", col("destination")));
        Dataset<Row> finalDataSet = notNullFields.select(col("TimeStamp"), col("destination"), col("user_ID"), col("user_ip"), col("day"));
//        finalDataSet = finalDataSet.filter(col("destination").when(col("destination").rlike("139.162.213.187:80"), "Telegram"));
        finalDataSet = finalDataSet.withColumnRenamed("user_ip", "userIP");
////                .select(col("destination").rlike(""))
//                .show(false);
//        System.out.println("finalDataset===");
        return finalDataSet;
    }

    public int getLogTypeID() throws Exception {
        int result;
        if (typeName.equals("Apache")) {
            result = 1;
        } else if (typeName.equals("MCI")) {
            result = 2;
        } else {
            throw new Exception("ERROR : LogType " + typeName + " is not defined");
        }

        return result;
    }

    private int makeUserID(String os, String browser, String ip) {
        throw new UnsupportedOperationException();
    }

    private String getReferrer(String referrer_fld) {
        throw new UnsupportedOperationException();
    }
}
