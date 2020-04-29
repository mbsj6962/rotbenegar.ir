/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//
package ir.ac.itrc.rotbenegar.DataFormats;

import java.util.Date;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class DatasetFactory {

    private static Encoder<DataFields> dataFieldsEncoder = Encoders.bean(DataFields.class);
    private static Encoder<CommonFields> commonFieldsEncoder = Encoders.bean(CommonFields.class);
    private static Encoder<Records> recordsEncoder = Encoders.bean(Records.class);
    private static Encoder<targetSiteTest> targetSiteTestEncoder = Encoders.bean(targetSiteTest.class);
    private static Encoder<targetSiteIDAndDomainTest> targetSiteIDAndDomainTestEncoder = Encoders.bean(targetSiteIDAndDomainTest.class);
    private static Encoder<DFWithAdditionalColumnTest> DFWithAdditionalColumnTestEncoder = Encoders.bean(DFWithAdditionalColumnTest.class);
    private static Encoder<outPutOfParse> outPutOfParseEncoder = Encoders.bean(outPutOfParse.class);
    private static Encoder<outPutClean> outPutCleanEncoder = Encoders.bean(outPutClean.class);
    private static Encoder<outPutPersistRank> outPutPersistRankEncoder = Encoders.bean(outPutPersistRank.class);
    private static Encoder<inputFile> inputFileEncoder = Encoders.bean(inputFile.class);
    
    public static Encoder<DataFields> getDataFieldsEncoder() {
        return dataFieldsEncoder;
    }

    public static Encoder<CommonFields> getCommonFieldsEncoder() {
        return commonFieldsEncoder;
    }

    public static Encoder<Records> getRecordsEncoder() {
        return recordsEncoder;
    }

    public static Encoder<targetSiteTest> getTargetSiteTestEncoder() {

        return targetSiteTestEncoder;

    }

    public static Encoder<targetSiteIDAndDomainTest> getTargetSiteIDAndDomainTestEncoder() {

        return targetSiteIDAndDomainTestEncoder;

    }

    public static Encoder<DFWithAdditionalColumnTest> getDFWithAdditionalColumnTestEncoder() {

        return DFWithAdditionalColumnTestEncoder;

    }

    public static Encoder<outPutOfParse> getOutPutOfParseEncoder() {

        return outPutOfParseEncoder;

    }

    public static Encoder<outPutClean> getOutPutCleanEncoder() {

        return outPutCleanEncoder;

    }

    public static Encoder<outPutPersistRank> getOutPutPersistRankEncoder() {

        return outPutPersistRankEncoder;
    }

    public static Encoder<inputFile> getInputFileEncoder(){
    
    return inputFileEncoder;
    
    }
    
    public static class inputFile {

        String start_time;
        String user_ip;
        String dst_ip;
        String host;
        long up_bytes;
        long down_bytes;
        String user_agent;
        String uri;
        String content_type;
        String referer;

        public inputFile(String start_time, String user_ip, String dst_ip, String host,
                long up_bytes, long down_bytes, String user_agent, String uri, String content_type, String referer) {

            this.start_time = start_time;
            this.user_ip = user_ip;
            this.dst_ip = dst_ip;
            this.host = host;
            this.up_bytes = up_bytes;
            this.down_bytes = down_bytes;
            this.user_agent = user_agent;
            this.uri = uri;
            this.content_type = content_type;
            this.referer = referer;

        }

        public String getStart_Time() {

            return start_time;

        }

        public void setStart_Time(String start_time) {

            this.start_time = start_time;

        }

        public String getUser_Ip() {

            return user_ip;

        }

        public void setUser_Ip(String user_ip) {

            this.user_ip = user_ip;

        }

        public String getDst_Ip() {

            return dst_ip;

        }

        public void setDst_ip(String dst_ip) {
            
            this.dst_ip=dst_ip;
            
        }

        public String getHost(){
        
            return host;
        
        }
        
        public void setHost(String host){
        
            this.host=host;
            
        }
        
        public long getUp_Bytes(){
        
        return up_bytes;
        
        }
        public void setUp_Bytes(long up_bytes){
        
        this.up_bytes=up_bytes;
        
        }
        
        public long getDown_Bytes(){
        
        return down_bytes;
        
        }
        
        public long setDown_Bytes(long down_bytes){
        
            return down_bytes;
        }
        
        public String getUser_Agent(){
        
            return user_agent;
            
        }
        
        public void setUser_Agent(String user_agent){
        
        this.user_agent=user_agent;
        
        }
        
        public String getUri(){
        
        return uri;
        
        }
        
        public void setUri(String uri){
        
        this.uri=uri;
        
        }
        
        public String getContentType(){
        
        return content_type;
        
        }
        
        public void setContentType(String content_type){
        
        this.content_type=content_type;
        
        }
        
        public String getReferer(){
        
        return referer;
        
        }
        
        public void setReferer(String referer){
        
        this.referer=referer;
        
        }
        
    }

    public static class outPutPersistRank {

        Date day;
        String domain;
        Double rankScore;
        long dailyVisitorCount;
        long dailyVisitCount;
        long dailySessionDuration;
        long dailySessionCount;
        String targetSiteID;
        int logTypeID;
        long rankNum;

        public outPutPersistRank(long dailySessionCount, long dailySessionDuration, long dailyVisitCount, long dailyVisitorCount,
                Date day, String domain, Double rankScore, String targetSiteID, int logTypeID, long rankNum) {

            this.day = day;
            this.domain = domain;
            this.rankScore = rankScore;
            this.dailyVisitorCount = dailyVisitorCount;
            this.dailyVisitCount = dailyVisitCount;
            this.dailySessionDuration = dailySessionDuration;
            this.dailySessionCount = dailySessionCount;
            this.targetSiteID = targetSiteID;
            this.logTypeID = logTypeID;
            this.rankNum = rankNum;
        }

        public Date getDay() {

            return day;

        }

        public void setDay(Date day) {

            this.day = day;

        }

        public String getDomain() {

            return domain;

        }

        public void setDomain(String domain) {

            this.domain = domain;

        }

        public Double getRankScore() {

            return rankScore;

        }

        public void setRankScore(Double rankScore) {

            this.rankScore = rankScore;
        }

        public long getDailyVisitorCount() {
            return dailyVisitorCount;
        }

        public void setDailyVisitorCount(long dailyVisitorCount) {

            this.dailyVisitorCount = dailyVisitorCount;

        }

        public long getDailyVisitCount() {

            return dailyVisitCount;

        }

        public void setDailyVisitCount(long dailyVisitCount) {

            this.dailyVisitCount = dailyVisitCount;

        }

        public long getDailySessionDuration() {

            return dailySessionDuration;

        }

        public void setDailySessionDuration(long dailySessionDuration) {

            this.dailySessionDuration = dailySessionDuration;

        }

        public long getDailySessionCount() {

            return dailySessionCount;

        }

        public void setDailySessionCount(long dailySessionCount) {

            this.dailySessionCount = dailySessionCount;

        }

        public String getTargetSiteID() {

            return targetSiteID;

        }

        public void setTargetSiteID(String targetSiteID) {

            this.targetSiteID = targetSiteID;

        }

        public int getLogTypeID() {

            return logTypeID;

        }

        public void setLogTypeID(int logTypeID) {

            this.logTypeID = logTypeID;

        }

        public long getRankNum() {

            return rankNum;

        }

        public void setRankNum(long rankNum) {

            this.rankNum = rankNum;

        }

    }

    public static class outPutClean {

        String TimeStamp;
        String user_ip;
        String user_ID;
        String destination;
        String day;

        public outPutClean(String TimeStamp, String user_ip, String user_ID, String destination, String day) {

            this.TimeStamp = TimeStamp;
            this.user_ip = user_ip;
            this.user_ID = user_ID;
            this.destination = destination;
            this.day = day;

        }

        public String getTimeStamp() {

            return TimeStamp;

        }

        public void setTimeStamp(String TimeStamp) {

            this.TimeStamp = TimeStamp;

        }

        public String getUser_Ip() {

            return user_ip;
        }

        public void setUser_Ip(String user_ip) {

            this.user_ip = user_ip;

        }

        public String getUser_ID() {

            return user_ID;

        }

        public void setUser_ID(String user_ID) {

            this.user_ID = user_ID;

        }

        public String getDestination() {

            return destination;
        }

        public void setDestination(String destination) {

            this.destination = destination;

        }

        public String getDay() {

            return day;

        }

        public void setDay(String day) {

            this.day = day;

        }

    }

    public static class outPutOfParse {

        String TimeStamp;
        String user_ip;
        String destination_ip;
        String destination_host;
        String user_ID;

        public outPutOfParse(String TimeStamp, String user_ip, String destination_ip, String destination_host, String user_ID) {

            this.TimeStamp = TimeStamp;
            this.user_ip = user_ip;
            this.destination_ip = destination_ip;
            this.destination_host = destination_host;
            this.user_ID = user_ID;

        }

        public String getTimeStamp() {

            return TimeStamp;

        }

        public void setTimeStamp(String TimeStamp) {

            this.TimeStamp = TimeStamp;

        }

        public String getUser_Ip() {

            return user_ip;
        }

        public void setUser_Ip(String user_ip) {

            this.user_ip = user_ip;

        }

        public String getDestination_Ip() {

            return destination_ip;

        }

        public void setDestination_Ip(String destination_ip) {

            this.destination_ip = destination_ip;

        }

        public String getDestination_Host() {

            return destination_host;

        }

        public void setDestination_Host(String destination_host) {

            this.destination_host = destination_host;

        }

        public String getUser_ID() {

            return user_ID;

        }

        public void setUser_ID(String user_ID) {

            this.user_ID = user_ID;

        }

    }

    public static class DFWithAdditionalColumnTest {

        long dailySessionCount;
        long dailySessionDuration;
        long dailyVisitCount;
        long dailyVisitorCount;
        long day;
        String domain;
        Double rankScore;
        String targetSiteID;
        long logTypeID;
        long rankNum;

        public DFWithAdditionalColumnTest(long dailySessionCount, long dailySessionDuration, long dailyVisitCount, long dailyVisitorCount,
                long day, String domain, Double rankScore, String targetSiteID, long logTypeID, long rankNum) {

            this.dailySessionCount = dailySessionCount;
            this.dailySessionDuration = dailySessionDuration;
            this.dailyVisitCount = dailyVisitCount;
            this.dailyVisitorCount = dailyVisitorCount;
            this.day = day;
            this.domain = domain;
            this.rankScore = rankScore;
            this.targetSiteID = targetSiteID;
            this.logTypeID = logTypeID;
            this.rankNum = rankNum;

        }

        public long getDailySessionCount() {

            return dailySessionCount;

        }

        public void setDailySessionCount(long dailySessionCount) {

            this.dailySessionCount = dailySessionCount;

        }

        public long getDailySessionDuration() {

            return dailySessionDuration;

        }

        public void setDailySessionDuration(long dailySessionDuration) {

            this.dailySessionDuration = dailySessionDuration;

        }

        public long getDailyVisitCount() {

            return dailyVisitCount;

        }

        public void setDailyVisitCount(long dailyVisitCount) {

            this.dailyVisitCount = dailyVisitCount;

        }

        public long getDailyVisitorCount() {
            return dailyVisitorCount;
        }

        public void setDailyVisitorCount(long dailyVisitorCount) {

            this.dailyVisitorCount = dailyVisitorCount;

        }

        public long getDay() {

            return day;

        }

        public void setDay(long day) {

            this.day = day;

        }

        public String getDomain() {

            return domain;

        }

        public void setDomain(String domain) {

            this.domain = domain;

        }

        public Double getRankScore() {

            return rankScore;

        }

        public void setRankScore(Double rankScore) {

            this.rankScore = rankScore;
        }

        public String getTargetSiteID() {
            return targetSiteID;
        }

        public void setTargetSiteID(String targetSiteID) {

            this.targetSiteID = targetSiteID;

        }

        public long getLogTypeID() {

            return logTypeID;
        }

        public void setLogTypeID(long logTypeID) {
            this.logTypeID = logTypeID;
        }

        public long getRankNum() {

            return rankNum;

        }

        public void setRankNum(long rankNum) {

            this.rankNum = rankNum;
        }
    }

    public static class targetSiteIDAndDomainTest {

        String targetSiteID;
        String domain;

        public targetSiteIDAndDomainTest(String targetSiteID, String domain) {

            this.targetSiteID = targetSiteID;
            this.domain = domain;

        }

        public String getTargetSiteID() {
            return targetSiteID;
        }

        public void setTargetSiteID(String targetSiteID) {

            this.targetSiteID = targetSiteID;

        }

        public String getDomain() {

            return domain;

        }

        public void setDomain(String domain) {

            this.domain = domain;

        }
    }

    public static class targetSiteTest {

        Long dailySessionCount;
        Long dailySessionDuration;
        Long dailyVisitCount;
        Long dailyVisitorCount;
        Long day;
        String domain;
        Double rankScore;

        public targetSiteTest(long dailySessionCount, long dailySessionDuration, long dailyVisitCount, long dailyVisitorCount,
                long day, String domain, Double rankScore) {

            this.day = day;
            this.domain = domain;
            this.rankScore = rankScore;
            this.dailySessionCount = dailySessionCount;
            this.dailySessionDuration = dailySessionDuration;
            this.dailyVisitCount = dailyVisitCount;
            this.dailyVisitorCount = dailyVisitorCount;

        }

        public long getDay() {

            return day;

        }

        public void setDay(long day) {

            this.day = day;

        }

        public String getDomain() {

            return domain;

        }

        public void setDomain(String domain) {

            this.domain = domain;

        }

        public Double getRankScore() {

            return rankScore;

        }

        public void setRankScore(Double rankScore) {

            this.rankScore = rankScore;
        }

        public long getDailyVisitorCount() {
            return dailyVisitorCount;
        }

        public void setDailyVisitorCount(long dailyVisitorCount) {

            this.dailyVisitorCount = dailyVisitorCount;

        }

        public long getDailyVisitCount() {

            return dailyVisitCount;

        }

        public void setDailyVisitCount(long dailyVisitCount) {

            this.dailyVisitCount = dailyVisitCount;

        }

        public long getDailySessionDuration() {

            return dailySessionDuration;

        }

        public void setDailySessionDuration(long dailySessionDuration) {

            this.dailySessionDuration = dailySessionDuration;

        }

        public long getDailySessionCount() {

            return dailySessionCount;

        }

        public void setDailySessionCount(long dailySessionCount) {

            this.dailySessionCount = dailySessionCount;

        }

    }

    public static class Records {

        long day;
        String domain;
        Double rankScore;
        long dailyVisitorCount;
        long dailyVisitCount;
        long dailySessionDuration;
        long dailySessionCount;

        public Records(long dailySessionCount, long dailySessionDuration, long dailyVisitCount, long dailyVisitorCount,
                long day, String domain, Double rankScore) {

            this.day = day;
            this.domain = domain;
            this.rankScore = rankScore;
            this.dailyVisitorCount = dailyVisitorCount;
            this.dailyVisitCount = dailyVisitCount;
            this.dailySessionDuration = dailySessionDuration;
            this.dailySessionCount = dailySessionCount;

        }

        public long getDay() {

            return day;

        }

        public void setDay(long day) {

            this.day = day;

        }

        public String getDomain() {

            return domain;

        }

        public void setDomain(String domain) {

            this.domain = domain;

        }

        public Double getRankScore() {

            return rankScore;

        }

        public void setRankScore(Double rankScore) {

            this.rankScore = rankScore;
        }

        public long getDailyVisitorCount() {
            return dailyVisitorCount;
        }

        public void setDailyVisitorCount(long dailyVisitorCount) {

            this.dailyVisitorCount = dailyVisitorCount;

        }

        public long getDailyVisitCount() {

            return dailyVisitCount;

        }

        public void setDailyVisitCount(long dailyVisitCount) {

            this.dailyVisitCount = dailyVisitCount;

        }

        public long getDailySessionDuration() {

            return dailySessionDuration;

        }

        public void setDailySessionDuration(long dailySessionDuration) {

            this.dailySessionDuration = dailySessionDuration;

        }

        public long getDailySessionCount() {

            return dailySessionCount;

        }

        public void setDailySessionCount(long dailySessionCount) {

            this.dailySessionCount = dailySessionCount;

        }
    }

    public static class CommonFields {

        protected long day;
        protected String domain;
        protected long timestamp;
        protected String userID;
        protected String userIP;

        public CommonFields(long day, String domain, long timestamp, String userID, String userIP) {
            this.day = day;
            this.domain = domain;
            this.timestamp = timestamp;
            this.userID = userID;
            this.userIP = userIP;
        }

        public long getDay() {
            return day;
        }

        public void setDay(long day) {
            this.day = day;
        }

        public String getDomain() {
            return domain;
        }

        public void setDomain(String domain) {
            this.domain = domain;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public String getUserID() {
            return userID;
        }

        public void setUserID(String userID) {
            this.userID = userID;
        }

        public String getUserIP() {
            return userIP;
        }

        public void setUserIP(String userIP) {
            this.userIP = userIP;
        }
    }

    public static class CommonFieldsAndBrowserOsRef extends CommonFields {

        protected String browser;
        protected String os;
        protected String ref;

        public CommonFieldsAndBrowserOsRef(
                long day, String domain, long timestamp, String userID, String userIP,
                String browser, String os, String ref) {

            super(day, domain, timestamp, userID, userIP);

            this.browser = browser;
            this.os = os;
            this.ref = ref;
        }

        public String getBrowser() {
            return browser;
        }

        public void setBrowser(String browser) {
            this.browser = browser;
        }

        public String getOs() {
            return os;
        }

        public void setOs(String os) {
            this.os = os;
        }

        public String getRef() {
            return ref;
        }

        public void setRef(String ref) {
            this.ref = ref;
        }
    }

    public static class DataFields extends CommonFieldsAndBrowserOsRef {

        public DataFields(
                long day, String domain, long timestamp, String userID, String userIP,
                String browser, String os, String ref) {
            super(day, domain, timestamp, userID, userIP, browser, os, ref);
        }
    }
//    public static class DataFields extends CommonFields {
//        public DataFields(
//                long day, String domain, long timestamp, String userID, String userIP)
//        {
//            super(day, domain, timestamp, userID, userIP);
//        }
//    }

    private static Encoder<ResultingFieldsForMeasuring> resultingFieldsEncoder = Encoders.bean(ResultingFieldsForMeasuring.class);

    public static Encoder<ResultingFieldsForMeasuring> getResultingFieldsForMeasuringEncoder() {
        return resultingFieldsEncoder;
    }

    public static class ResultingFieldsForMeasuring {

        protected Integer logTypeID;
        protected long day;
        protected String domainID;
        protected Integer criterionID;
        protected long count;

        public ResultingFieldsForMeasuring(Integer logTypeID, long day, String domainID, Integer criterionID, long count) {
            this.logTypeID = logTypeID;
            this.day = day;
            this.domainID = domainID;
            this.criterionID = criterionID;
            this.count = count;
        }

        public Integer getLogTypeID() {
            return logTypeID;
        }

        public void setLogTypeID(Integer logTypeID) {
            this.logTypeID = logTypeID;
        }

        public long getDay() {
            return day;
        }

        public void setDay(long day) {
            this.day = day;
        }

        public String getDomainID() {
            return domainID;
        }

        public void setDomainID(String domainID) {
            this.domainID = domainID;
        }

        public Integer getCriterionID() {
            return criterionID;
        }

        public void setCriterionID(Integer criterionID) {
            this.criterionID = criterionID;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

    }

    private static Encoder<HashMapFields> hashMapFieldsEncoder = Encoders.bean(HashMapFields.class);
    public static Encoder<HashMapFields> getHashMapFieldsEncoder() {
        return hashMapFieldsEncoder;
    }

    public static class HashMapFields {
        protected String key;
        protected Integer value;
        
        public HashMapFields(String key, Integer value) {
            this.key = key;
            this.value = value;
        }
        
        public String getKey() {
            return key;
        }
        
        public void setKey(String key) {
            this.key = key;
        }

        public Integer getValue() {
            return value;
        }
        
        public void setValue(Integer value) {
            this.value = value;
        }
    }

}
