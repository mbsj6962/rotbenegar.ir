package ir.ac.itrc.rotbenegar.Pipeline;

import java.util.Map;
import ir.ac.itrc.rotbenegar.Utilities.SparkHandlers;
import ir.ac.itrc.rotbenegar.Utilities.Logger;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.spark.broadcast.Broadcast;
import com.google.common.net.InternetDomainName;
import ir.ac.itrc.rotbenegar.Ranking.HostExtract;
import java.net.URLDecoder;

public class URL implements java.io.Serializable {

    private static Broadcast<Map<String, String>> ip2DomainMap;

    public URL() throws IOException {

        String mapPath = "Ip2Domain_Map.txt";
        //System.out.println(mapPath+ "= path");
        if (mapPath == null) {
            String msg = mapPath + " ip2DomainMap Path";
            Logger.write(msg);
            // throw new IOException(msg);
        } else {
            ip2DomainMap = loadip2DomainMap(mapPath);
        }
        // System.out.println(ip2DomainMap);
        /* try{
            
            ip2DomainMap = loadip2DomainMap(mapPath);
        } catch (Exception e){
            String msg = mapPath + " NOT found!";

            Logger.write(msg);
            throw new IOException(msg);
        }*/

        //DataFiles.getIP2DomainDataPath();
        //JavaSparkContext sc = SparkHandlers.getJavaSparkContext();
        /*SparkConf conf = new SparkConf().setAppName("Webranking").setMaster("local[12]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> data = sc.textFile(path);

        JavaPairRDD<String, String> javaRDDPair = data.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(final String line) throws Exception {
                String key = line.split("\t")[0];
                String value = line.split("\t")[1];
                return new Tuple2<String, String>(key, value);
            }
        });

        Map<String, String> map = javaRDDPair.collectAsMap();
        ip2domainMap = new HashMap<>(javaRDDPair.collectAsMap());*/
    }

    public static Broadcast<Map<String, String>> loadip2DomainMap(String filename) {
        if (filename == null || filename == "") {
            return null;
        }
        try {
            BufferedReader reader = readFileFromResources(filename);
            Map<String, String> map = new HashMap<String, String>();

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.equals("")) {
                    continue;
                }

                String parts[] = line.split("\t");

                assert parts.length == 2;

                map.put(parts[0].trim().toLowerCase(), parts[1].trim().toLowerCase());
            }
            reader.close();

            return SparkHandlers.getJavaSparkContext().broadcast(map);

        } catch (IOException e) {
            Logger.write("Exception ", e);

            return null;
        }
    }

    private static BufferedReader readFileFromResources(String filename) throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream(filename);
        System.out.println(input);

        if (input == null) {
            String msg = filename + " NOT found!";

            Logger.write(msg);
            throw new IOException(msg);
        }

        return new BufferedReader(new InputStreamReader(input, "UTF-8"));
    }

    public String extractDomain(String destination_ip, String destination_host) throws IOException, Exception {
        if (destination_ip == null || destination_ip.isEmpty()) {
            return null; //remove tuple
        } else if (validIpPort(destination_host)) {
            return destination_host;
        } else if (validIP(destination_host)) {
            return destination_host;
        } else if (destination_host == null) {
            String domain = ip2Domain(destination_ip);
            if (domain == null || domain.isEmpty()) {
                return null;
            } else {
                return domain.trim().toLowerCase();
            }

        } else {
            String domain = extractTLD(destination_host);
            if (domain != null) {
                return domain;//TLD
            }
        }

        return null;//remove tuple

    }

    public String ip2Domain(String ip) {
        if (ip2DomainMap == null) {
            return null;
        }
        if (ip2DomainMap.value().containsKey(ip)) {
            return ip2DomainMap.value().get(ip);
        } else {
            Logger.write("Does Not Exist in Ip2DomainMap");
            return null;
        }
    }

    public static String extractTLD(String host) throws Exception {
        try {
            java.net.URL url = new java.net.URL(URLDecoder.decode(host,"UTF-8"));
            String domain = HostExtract.getHost(url.toExternalForm());
            return domain;
        } catch (Exception e) {
            Logger.write("Not Valid Domain for " + host);
        }
        return null;
    }

    public static boolean validIP(String host) {
        if (host == null) {
            return false;
        }
        try {
            if (host == null || host.isEmpty()) {
                return false;
            }

            String[] parts = host.split("\\.");
            if (parts.length != 4) {
                return false;
            }

            for (String s : parts) {
                int i = Integer.parseInt(s);
                if ((i < 0) || (i > 255)) {
                    return false;
                }
            }
            if (host.endsWith(".")) {
                return false;
            }

            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }

    public static boolean validIpPort(String host) {
        if (host == null) {
            return false;
        }
        if (host.contains(":")) {
            String[] port = host.split(":");
            if (port.length != 2) {
                return false;
            } else if (!validIP(port[0])) {
                return false;
            } else if (port[1].matches("[0-9]+")) {
                int i = Integer.parseInt(port[1]);
                if (i < 1 || i > 6536) {
                    return false;
                }
                return true;
            }
        }
        if (validIP(host)) {
            return true;
        } else {
            return false;
        }

    }

    public static String getHost(String urlString) throws Exception {

        String url = urlString;
        url = url.trim().replace("\n", "").replace("\r", "").toLowerCase();

        int doubleslash = url.indexOf("//");
        if (doubleslash == -1) {
            doubleslash = 0;
        } else {
            doubleslash += 2;
        }

        int slash = url.indexOf('/', doubleslash);
        if (slash == -1) {
            slash = url.length();
        }

        int space = url.indexOf(' ', doubleslash);
        if (space == -1) {
            space = url.length();
        }

        int questionMark = url.indexOf('?', doubleslash);
        if (questionMark == -1) {
            questionMark = url.length();
        }

        int numberSign = url.indexOf('#', doubleslash);
        if (numberSign == -1) {
            numberSign = url.length();
        }

        int ampersand = url.indexOf('&', doubleslash);
        if (ampersand == -1) {
            ampersand = url.length();
        }

        int colonSign = url.indexOf(':', doubleslash);
        if (colonSign == -1) {
            colonSign = url.length();
        }

        int end = slash < space ? slash : space;
        end = end < questionMark ? end : questionMark;
        end = end < numberSign ? end : numberSign;
        end = end < ampersand ? end : ampersand;
        end = end < colonSign ? end : colonSign;

        String host = url.substring(doubleslash, end);

        if (host.startsWith("www.") && (host.length() > 4)) {
            host = host.substring(4);

        }
        String[] domainNames = host.split("\\.");
        if (domainNames.length <= 2) {
            return host;
        } else {
            // The domain name part should not start or end with dash (-) (e.g. -google-.com)
            Boolean badname = false;
            for (int index = 0; index < domainNames.length; index++) {
                if (domainNames[index].startsWith("-") || domainNames[index].startsWith("_") || domainNames[index].endsWith("-") || domainNames[index].endsWith("_")) {
                    badname = true;
                }
            }
            if (badname) {
                return "INVALID_DOMAIN";
            }
            try {
                InternetDomainName internatDomainName = InternetDomainName.from(host);

                if (internatDomainName.isUnderPublicSuffix()) {
                    return internatDomainName.topPrivateDomain().toString().replace("InternetDomainName{name=", "").replace("}", "");
                } else {
                    return "INVALID_DOMAIN";
                }
            } catch (Exception e) {
                //check if it is a number
                Boolean flag = true;
                for (int i = 0; i < domainNames.length; i++) {
                    flag = flag & isInteger(domainNames[i]);
                }
                if (domainNames.length == 4) {
                    if (Integer.parseInt(domainNames[0]) < 256 && Integer.parseInt(domainNames[1]) < 256 && Integer.parseInt(domainNames[2]) < 256 && Integer.parseInt(domainNames[3]) < 256) {
                        return host;
                    }
                }
                throw e;
            }
        }

    }

    private static boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return false;
        } catch (NullPointerException e) {
            return false;
        }
        // only got here if we didn't return false
        return true;
    }

}
