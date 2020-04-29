/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ir.ac.itrc.rotbenegar.Pipeline;

import java.io.IOException;
import java.util.Map;
import org.apache.spark.broadcast.Broadcast;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author bagheri
 */
public class URLTest {

    public URLTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /* Test of loadip2DomainMap method, of class URLParser. */
    @Test
    public void testEmptyPathLoadip2DomainMap() throws IOException {
        System.out.println("loadip2DomainMap");
        String filename = "";
        Broadcast<Map<String, String>> expResult = null;
        URLParser instance = new URLParser();
        Broadcast<Map<String, String>> result = instance.loadip2DomainMap(filename);
        assertEquals(expResult, result);
    }

    @Test
    public void testNullPathLoadip2DomainMap() throws IOException {
        System.out.println("loadip2DomainMap");
        String filename = null;
        Broadcast<Map<String, String>> expResult = null;
        URLParser instance = new URLParser();
        Broadcast<Map<String, String>> result = instance.loadip2DomainMap(filename);
        assertEquals(expResult, result);
    }

    @Test
    public void testWrongPathLoadip2DomainMap() throws IOException {
        System.out.println("loadip2DomainMap");
        String filename = "jhgvbftgc";
        Broadcast<Map<String, String>> expResult = null;
        URLParser instance = new URLParser();
        Broadcast<Map<String, String>> result = instance.loadip2DomainMap(filename);
        assertEquals(expResult, result);
    }

    /* Test of ExtractDomain method, of class URLParser.  
    
IP      Host    result
---------------------------------------
Null            Any	Delete this tuple
some value	Null	DNS-RESOLVE
some value	ip:port	ip:port
some value	ip	ip
some value	text (without TLD)	Delete this tuple
     */
 /*IP = Null Host = Any : result--> Null*/
    @Test
    public void testExtractDomain1() throws Exception {
        System.out.println("extractDomain");
        String destination_ip = null;
        String destination_host = "";
        URLParser instance = new URLParser();
        String expResult = null;
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);
    }

    @Test
    public void testExtractDomain2() throws Exception {
        System.out.println("extractDomain");
        String destination_ip = null;
        String destination_host = null;
        URLParser instance = new URLParser();
        String expResult = null;
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);

    }

    @Test
    public void testExtractDomain3() throws Exception {
        System.out.println("extractDomain");
        String destination_ip = null;
        String destination_host = "1.1.1.1";
        URLParser instance = new URLParser();
        String expResult = null;
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);

    }

    @Test
    public void testExtractDomain4() throws Exception {
        System.out.println("extractDomain");
        String destination_ip = null;
        String destination_host = "1.1.1.1:120";
        URLParser instance = new URLParser();
        String expResult = null;
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);

    }

    @Test
    public void testExtractDomain5() throws Exception {
        System.out.println("extractDomain");
        String destination_ip = null;
        String destination_host = "itrc.ac.ir";
        URLParser instance = new URLParser();
        String expResult = null;
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);

    }

    @Test
    public void testExtractDomain6() throws Exception {
        System.out.println("extractDomain");
        String destination_ip = null;
        String destination_host = "dsfwsdferfeefwe";
        URLParser instance = new URLParser();
        String expResult = null;
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);
    }

    /* IP = Any  Host = Null: result--> DNS-RESOLVE*/
    @Test
    public void testExtractDomain7() throws Exception {
        System.out.println("extractDomain");
        String destination_ip = "";
        String destination_host = null;
        URLParser instance = new URLParser();
        String expResult = null;
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);

    }

    @Test
    public void testExtractDomain8() throws Exception {
        System.out.println("extractDomain");
        URLParser instance = new URLParser();

        String destination_ip = "10.1.1.1";
        String destination_host = null;

        String expResult = "test.ir";
        String result = instance.extractDomain(destination_ip, destination_host);

        assertEquals(expResult, result);
    }

    @Test
    public void testExtractDomain9() throws Exception {
        System.out.println("extractDomain");
        String destination_ip = "20.1.1.1.1";
        String destination_host = null;
        URLParser instance = new URLParser();
        String expResult = null;
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);

    }

    @Test
    public void testExtractDomain10() throws Exception {
        System.out.println("extractDomain");
        String destination_ip = "20.1.1.1";
        String destination_host = null;
        URLParser instance = new URLParser();
        String expResult = null;
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);

    }

    /* IP = Any(not Null)  Host = IP:Port: result--> IP:Port*/
    @Test
    public void testExtractDomain11() throws Exception {
        System.out.println("extractDomain");
        String destination_host = "10.1.1.1:80";
        String destination_ip = "1.1.1.1";
        URLParser instance = new URLParser();
        String expResult = "10.1.1.1:80";
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);
    }

    @Test
    public void testExtractDomain12() throws Exception {
        System.out.println("extractDomain");
        String destination_host = "10.1.1.1:80";
        String destination_ip = "1.1.1.1:20";
        URLParser instance = new URLParser();
        String expResult = "10.1.1.1:80";
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);
    }

    public void testExtractDomain13() throws Exception {
        System.out.println("extractDomain");
        String destination_host = "10.1.1.1:80";
        String destination_ip = "test.ir";
        URLParser instance = new URLParser();
        String expResult = "10.1.1.1:80";
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);
    }

    /* IP = Any(not Null)  Host = IP result--> IP*/
    @Test
    public void testExtractDomain14() throws Exception {
        System.out.println("extractDomain");
        String destination_host = "10.1.1.1";
        String destination_ip = "1.1.1.1";
        URLParser instance = new URLParser();
        String expResult = "10.1.1.1";
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);
    }

    @Test
    public void testExtractDomain15() throws Exception {
        System.out.println("extractDomain");
        String destination_host = "10.1.1.1";
        String destination_ip = "1.1.1.1:20";
        URLParser instance = new URLParser();
        String expResult = "10.1.1.1";
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);
    }

    public void testExtractDomain16() throws Exception {
        System.out.println("extractDomain");
        String destination_host = "10.1.1.1";
        String destination_ip = "test.ir";
        URLParser instance = new URLParser();
        String expResult = "10.1.1.1";
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);
    }

    /* IP = Any(not Null)  Host = url : result--> domain name or null*/
    @Test
    public void testExtractDomain17() throws Exception {
        System.out.println("extractDomain");
        String destination_host = "mail.google.com";
        String destination_ip = "1323213";
        URLParser instance = new URLParser();
        String expResult = "google.com";
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);
    }

    @Test
    public void testExtractDomain18() throws Exception {
        System.out.println("extractDomain");
        String destination_host = "https://beginnersbook.com/2013/04/try-catch-in-java/";
        String destination_ip = "1323213";
        URLParser instance = new URLParser();
        String expResult = "beginnersbook.com";
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);
    }

    @Test
    public void testExtractDomain19() throws Exception {
        System.out.println("extractDomain");
        String destination_host = "https://beginnersbook.com/2013/04/try-catch-in-java/";
        String destination_ip = "13.23.21.3";
        URLParser instance = new URLParser();
        String expResult = "beginnersbook.com";
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);
    }
     @Test
    public void testExtractDomain20() throws Exception {
        System.out.println("extractDomain");
        String destination_host = "mail";
        String destination_ip = "1323213";
        URLParser instance = new URLParser();
        String expResult = null;
        String result = instance.extractDomain(destination_ip, destination_host);
        assertEquals(expResult, result);
    }

    /**
     * Test of validIP method, of class URLParser. true for valid IP or valid IP:PORT
     */
    @Test
    public void testvalidIpPort1() throws IOException {
        System.out.println("validIpPort");
        String host = "";
        boolean expResult = false;
        URLParser instance = new URLParser();
        boolean result
                = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort2() throws IOException {
        System.out.println("validIpPort");
        String host = null;
        boolean expResult = false;
        URLParser instance = new URLParser();
        boolean result
                = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort3() throws IOException {
        System.out.println("validIpPort");
        String host = "10.1.1.1";
        boolean expResult = true;
        URLParser instance = new URLParser();
        boolean result
                = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort4() throws IOException {
        System.out.println("validIpPort");
        String host = "10.1.1.1:80";
        boolean expResult = true;
        URLParser instance = new URLParser();
        boolean result
                = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort5() throws IOException {
        System.out.println("validIpPort");
        String host = "101.1.1";
        boolean expResult = false;
        URLParser instance = new URLParser();
        boolean result
                = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort6() throws IOException {
        System.out.println("validIpPort");
        String host = "10000.1.1.1";
        boolean expResult = false;
        URLParser instance = new URLParser();
        boolean result
                = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort7() throws IOException {
        System.out.println("validIpPort");
        String host = "10.1.1.1.";
        boolean expResult = false;
        URLParser instance = new URLParser();
        boolean result
                = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort8() throws IOException {
        System.out.println("validIpPort");
        String host = ".10.1.1.1";
        boolean expResult = false;
        URLParser instance = new URLParser();
        boolean result
                = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort9() throws IOException {
        System.out.println("validIpPort");
        String host = ".10.1.1.1.6";
        boolean expResult = false;
        URLParser instance = new URLParser();
        boolean result
                = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort10() throws IOException {
        System.out.println("validIpPort");
        String host = "101.1.a.2";
        boolean expResult = false;
        URLParser instance = new URLParser();
        boolean result = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort11() throws IOException {
        System.out.println("validIpPort");
        String host = "101.1.3.2:80";
        boolean expResult = true;
        URLParser instance = new URLParser();
        boolean result = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort12() throws IOException {
        System.out.println("validIpPort");
        String host = "101.1.:1.2";
        boolean expResult = false;
        URLParser instance = new URLParser();
        boolean result = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort13() throws IOException {
        System.out.println("validIpPort");
        String host = "01.1.3.2:a";
        boolean expResult = false;
        URLParser instance = new URLParser();
        boolean result = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort14() throws IOException {
        System.out.println("validIpPort");
        String host = "12:101.1.4.2";
        boolean expResult = false;
        URLParser instance = new URLParser();
        boolean result = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort15() throws IOException {
        System.out.println("validIpPort");
        String host = "101.1.5.a";
        boolean expResult = false;
        URLParser instance = new URLParser();
        boolean result = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort16() throws IOException {
        System.out.println("validIpPort");
        String host = "10.1.1.1:80";
        boolean expResult = true;
        URLParser instance = new URLParser();
        boolean result = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

    @Test
    public void testvalidIpPort17() throws IOException {
        System.out.println("validIpPort");
        String host = "10.1.1.1:8020";
        boolean expResult = false;
        URLParser instance = new URLParser();
        boolean result = instance.validIpPort(host);
        assertEquals(expResult, result);
    }

}
