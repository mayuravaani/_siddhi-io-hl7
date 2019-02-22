/*
 *  Copyright (c) 2019 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.extension.siddhi.io.hl7.source;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.hl7.util.TestUtil;
import org.wso2.extension.siddhi.io.hl7.util.UnitTestAppender;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.io.File;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfHl7Source {

    private static Logger log = Logger.getLogger(TestCaseOfHl7Source.class);
    private AtomicInteger count = new AtomicInteger();
    private volatile boolean eventArrived;
    private List<String> receivedEvent;
    private int timeout = 10000;
    private int waitTime = 50;
    private PipeParser pipeParser = new PipeParser();
    private TestUtil testUtil = new TestUtil();

    @BeforeMethod
    private void setUP() {

        eventArrived = false;
        count.set(0);
        File keyStoreFilePath = new File("src/test");
        String keyStorePath = keyStoreFilePath.getAbsolutePath();
        System.setProperty("carbon.home", keyStorePath);
    }

    @Test
    public void hl7ConsumerTestER7() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source to test multiple events with different type of ER7 format messages - text mapping");
        log.info("---------------------------------------------------------------------------------------------");
        receivedEvent = new ArrayList<>(3);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source( type = 'hl7',\n" +
                "port = '5041',\n" +
                "hl7.encoding = 'ER7',\n" +
                "@map(type = 'text'))\n" +
                "define stream hl7stream (payload string);\n";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.addCallback("hl7stream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                for (Event event : events) {
                    eventArrived = true;
                    count.incrementAndGet();
                    log.info(event.toString().replaceAll("\r", "\n"));
                    try {
                        Message message = pipeParser.parse(event.getData(0).toString());
                        receivedEvent.add(testUtil.getControlID(message));
                    } catch (HL7Exception e) {
                        log.error(e);
                    }
                }
            }
        });
        String siddhiApp1 = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5041', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7sinkStream(payload string);";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        InputHandler stream = executionPlanRuntime.getInputHandler("hl7sinkStream");
        executionPlanRuntime.start();
        String payLoadER71 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A04|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A04|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r" +
                "PID|1||123456789|0123456789^AA^^JP|BROS^MARIO^^^^||19850101000000|M|||123 FAKE " +
                "STREET^MARIO \\T\\ LUIGI BROS PLACE^TOADSTOOL KINGDOM^NES^A1B2C3^JP^HOME^^1234|1234|" +
                "(555)555-0123^HOME^JP:1234567|||S|MSH|12345678|||||||0|||||N\r" +
                "NK1|1|PEACH^PRINCESS^^^^|SO|ANOTHER CASTLE^^TOADSTOOL KINGDOM^NES^^JP|(123)555-1234|" +
                "(123)555-2345|NOK|||||||||||||\r";
        String payLoadER72 = "MSH|^~\\&|||||20190122111442.228+0530||ORM^O01|6101|T|2.3\r";
        String payLoadER73 = "MSH|^~\\&|||||20190123062351.436+0530||ORU^R01^ORU_R01|6401|T|2.4\r";
        List<String> expected = new ArrayList<>(3);
        stream.send(new Object[]{payLoadER71});
        stream.send(new Object[]{payLoadER72});
        stream.send(new Object[]{payLoadER73});
        Thread.sleep(10000);
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER71)));
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER72)));
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER73)));
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        AssertJUnit.assertEquals(3, count.get());
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(expected, receivedEvent);
        siddhiAppRuntime.shutdown();
        executionPlanRuntime.shutdown();
    }

    @Test
    public void hl7ConsumerTestXMLADT() throws InterruptedException, HL7Exception {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test with XML format message - custom xml mapping for ADT message");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source ( type = 'hl7',\n" +
                "port = '5042',\n" +
                "hl7.encoding = 'xml',\n" +
                "@map (type = 'xml', namespaces='ns=urn:hl7-org:v2xml', @attributes(MSH10 = 'ns:MSH/ns:MSH.10', " +
                "MSH3HD1 = 'ns:MSH/ns:MSH.3/ns:HD.1', MSH12 = 'ns:MSH/ns:MSH.12', " +
                "EVNTS1 = 'ns:EVN/ns:EVN.2/ns:TS.1')))\n" +
                "define stream hl7stream (MSH10 string, MSH3HD1 string, MSH12 string, EVNTS1 string);\n";
        receivedEvent = new ArrayList<>(2);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.addCallback("hl7stream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count.incrementAndGet();
                    String message = event.getData(0).toString();
                    receivedEvent.add(message);
                }
            }
        });
        String siddhiApp1 = "@App:name('TestExecutionPlan')\n" +
                "@sink(type ='hl7', " +
                "uri = 'localhost:5042', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\"))) " +
                "define stream hl7sinkStream(payload string);";
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        InputHandler stream = executionPlanRuntime.getInputHandler("hl7sinkStream");
        executionPlanRuntime.start();
        String payLoadER71 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A01|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A01|20010101000000\r";
        String payLoadER72 = "MSH|^~\\&|SENDSYS|SENDFACILITY|TESTSYS|TESTFACILITY|20190123062351||ADT^A04|" +
                "M123768789T123456789X123456|P|2.3\r" +
                "EVN|A01|20190123062351\r";
        stream.send(new Object[]{payLoadER71});
        stream.send(new Object[]{payLoadER72});
        List<String> expected = new ArrayList<>(2);
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER71)));
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER72)));
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        AssertJUnit.assertEquals(2, count.get());
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(expected, receivedEvent);
        siddhiAppRuntime.shutdown();
        executionPlanRuntime.shutdown();
    }

    @Test
    public void hl7ConsumerTestXMLBAR() throws InterruptedException, HL7Exception {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test with XML format message - custom xml mapping for BAR message");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source ( type = 'hl7',\n" +
                "port = '5046',\n" +
                "hl7.encoding = 'xml',\n" +
                "@map (type = 'xml', namespaces='ns=urn:hl7-org:v2xml', @attributes(MSH1 = 'ns:MSH/ns:MSH.1', " +
                "MSH2 = 'ns:MSH/ns:MSH.2', MSH3HD1 = 'ns:MSH/ns:MSH.3/ns:HD.1', MSH4HD1 = 'ns:MSH/ns:MSH.4/ns:HD.1', " +
                "MSH5HD1 = 'ns:MSH/ns:MSH.5/ns:HD.1', MSH6HD1 = 'ns:MSH/ns:MSH.6/ns:HD.1', " +
                "MSH7TS1 = 'ns:MSH/ns:MSH.7/ns:TS.1', MSH9MSG1 = 'ns:MSH/ns:MSH.9/ns:CM_MSG.1', " +
                "MSH9MSG2 = 'ns:MSH/ns:MSH.9/ns:CM_MSG.2', MSH10 = 'ns:MSH/ns:MSH.10', " +
                "MSH11PT1 = 'ns:MSH/ns:MSH.11/ns:PT.1', MSH12 = 'ns:MSH/ns:MSH.12', " +
                "EVNTS1 = 'ns:EVN/ns:EVN.2/ns:TS.1')))\n" +
                "define stream hl7stream (MSH1 string, MSH2 string, MSH3HD1 string, MSH4HD1 string, MSH5HD1 string, " +
                "MSH6HD1 string, MSH7TS1 string, MSH9MSG1 string, MSH9MSG2 string, MSH10 string, " +
                "MSH11PT1 string, MSH12 string, EVNTS1 string);\n";
        receivedEvent = new ArrayList<>(2);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.addCallback("hl7stream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count.incrementAndGet();
                    String message = event.getData(9).toString();
                    receivedEvent.add(message);
                }
            }
        });
        String siddhiApp1 = "@App:name('TestExecutionPlan')\n" +
                "@sink(type ='hl7', " +
                "uri = 'localhost:5046', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\"))) " +
                "define stream hl7sinkStream(payload string);";
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        InputHandler stream = executionPlanRuntime.getInputHandler("hl7sinkStream");
        executionPlanRuntime.start();
        String payLoadER71 = "MSH|^~\\&|ADMISSION|ADT1|RADIOLOGY|MIR1|19930502184808||BAR^P01|589888ADT30502184808" +
                "|P|2.3\r" +
                "EVN|P01|19930502184802\r" +
                "PID|||429314294^1^M10|333437208|WASHINGTON-HARPER^VOLA^K||19541218|F||B|" +
                "2146 E HARRIS^\"\"^ST.LOUIS^MO^63107^\"\"|510|(314)389-8628|||M|C|429314294^1^M10|500-60-8950\r";
        String payLoadER72 = "MSH|^~\\&|ADMISSION|ADT1|RADIOLOGY|MIR1|19930502184009||BAR^P02|589888ADT3050333308" +
                "|P|2.3\r" +
                "EVN|P02|19930502184009\r" +
                "PID|||429314294^1^M10|333437208|WASHINGTON-HARPER^VOLA^K||19541218|F||B|" +
                "2146 E HARRIS^\"\"^ST.LOUIS^MO^63107^\"\"|510|(314)389-8628|||M|C|429314294^1^M10|500-60-8950";
        log.info("$$$$$$$$$$$" + pipeParser.parse(payLoadER71).toString());
        stream.send(new Object[]{payLoadER71});
        stream.send(new Object[]{payLoadER72});
        List<String> expected = new ArrayList<>(2);
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER71)));
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER72)));
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        AssertJUnit.assertEquals(2, count.get());
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(expected, receivedEvent);
        siddhiAppRuntime.shutdown();
        executionPlanRuntime.shutdown();
    }

    @Test
    public void hl7ConsumerTestAckEncoding() throws InterruptedException, HL7Exception {

        File keyStoreFilePath = new File("src/test");
        String keyStorePath = keyStoreFilePath.getAbsolutePath();
        System.clearProperty("carbon.home");
        System.setProperty("carbon.home", keyStorePath);
        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test with logging format of hl7 Acknowledgment message Type");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source ( type = 'hl7',\n" +
                "port = '5049',\n" +
                "hl7.encoding = 'xml',\n" +
                "hl7.ack.encoding = 'xml',\n" +
                "@map (type = 'xml', namespaces='ns=urn:hl7-org:v2xml', @attributes(MSH10 = \"ns:MSH/ns:MSH.10\"," +
                "MSH3HD1 = \"ns:MSH/ns:MSH.3/ns:HD.1\")))\n" +
                "define stream hl7stream (MSH10 string, MSH3HD1 string);\n";
        receivedEvent = new ArrayList<>(2);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.addCallback("hl7stream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count.incrementAndGet();
                    String message = event.getData(0).toString();
                    receivedEvent.add(message);
                }
            }
        });
        String siddhiApp1 = "@App:name('TestExecutionPlan')\n" +
                "@sink(type ='hl7', " +
                "uri = 'localhost:5049', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\"))) " +
                "define stream hl7sinkStream(payload string);";
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        InputHandler stream = executionPlanRuntime.getInputHandler("hl7sinkStream");
        executionPlanRuntime.start();
        String payLoadER71 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A01|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A01|20010101000000\r";
        String payLoadER72 = "MSH|^~\\&|SENDSYS|SENDFACILITY|TESTSYS|TESTFACILITY|20190123062351||ADT^A01|" +
                "M123768789T123456789X123456|P|2.3\r";
        stream.send(new Object[]{payLoadER71});
        stream.send(new Object[]{payLoadER72});
        List<String> expected = new ArrayList<>(2);
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER71)));
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER72)));
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        AssertJUnit.assertEquals(2, count.get());
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(expected, receivedEvent);
        siddhiAppRuntime.shutdown();
        executionPlanRuntime.shutdown();
    }

    @Test
    public void hl7ConsumerTestCharset() throws InterruptedException, HL7Exception {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test with different charset type");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source ( type = 'hl7',\n" +
                "port = '5046',\n" +
                "hl7.encoding = 'xml',\n" +
                "charset = 'ISO-8859-2',\n" + //if client is running on single byte encoding, utf-16 not supportable
                "@map (type = 'xml', namespaces='ns=urn:hl7-org:v2xml', @attributes(MSH10 = \"ns:MSH/ns:MSH.10\"," +
                "MSH3HD1 = \"ns:MSH/ns:MSH.3/ns:HD.1\")))\n" +
                "define stream hl7stream (MSH10 string, MSH3HD1 string);\n";
        receivedEvent = new ArrayList<>(2);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.addCallback("hl7stream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count.incrementAndGet();
                    String message = event.getData(0).toString();
                    receivedEvent.add(message);
                }
            }
        });
        String siddhiApp1 = "@App:name('TestExecutionPlan')\n" +
                "@sink(type ='hl7', " +
                "uri = 'localhost:5046', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\"))) " +
                "define stream hl7sinkStream(payload string);";
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        InputHandler stream = executionPlanRuntime.getInputHandler("hl7sinkStream");
        executionPlanRuntime.start();
        String payLoadER71 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A01|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A01|20010101000000\r";
        String payLoadER72 = "MSH|^~\\&|SENDSYS|SENDFACILITY|TESTSYS|TESTFACILITY|20190123062351||ADT^A01|" +
                "M123768789T123456789X123456|P|2.3\r";
        stream.send(new Object[]{payLoadER71});
        stream.send(new Object[]{payLoadER72});
        List<String> expected = new ArrayList<>(2);
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER71)));
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER72)));
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        AssertJUnit.assertEquals(2, count.get());
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(expected, receivedEvent);
        siddhiAppRuntime.shutdown();
        executionPlanRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void hl7ConsumerTestMissingMandatoryProperty() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test with missing mandatory parameters - hl7.encoding is missing");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source( type = 'hl7',\n" +
                "port = '5041',\n" +
                "@map(type = 'text'))\n" +
                "define stream hl7stream (payload string);\n";
        siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiManager.shutdown();
    }

    @Test(expectedExceptions = UnsupportedCharsetException.class)
    public void hl7ConsumerTestUnsupportedCharset() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test with given charset Type is invalid");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source ( type = 'hl7',\n" +
                "port = '5042',\n" +
                "hl7.encoding = 'xml',\n" +
                "charset = 'UTF_8',\n" +
                "@map (type = 'xml', namespaces='ns=urn:hl7-org:v2xml', @attributes(MSH10 = \"ns:MSH/ns:MSH.10\"," +
                "MSH3HD1 = \"ns:MSH/ns:MSH.3/ns:HD.1\")))\n" +
                "define stream hl7stream (MSH10 string, MSH3HD1 string);\n";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void hl7ConsumerTestMessageProcessException() throws InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test with unrecognized structure of received message - ");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Hl7ExceptionHandler.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source ( type = 'hl7',\n" +
                "port = '5091',\n" +
                "hl7.encoding = 'xml',\n" +
                "@map (type = 'xml', namespaces='ns=urn:hl7-org:v2xml', @attributes(MSH10 = \"ns:MSH/ns:MSH.10\"," +
                "MSH3HD1 = \"ns:MSH/ns:MSH.3/ns:HD.1\")))\n" +
                "define stream hl7stream (MSH10 string, MSH3HD1 string);\n";
        receivedEvent = new ArrayList<>(2);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.addCallback("hl7stream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count.incrementAndGet();
                    String message = event.getData(0).toString();
                    receivedEvent.add(message);
                }
            }
        });
        String siddhiApp1 = "@App:name('TestExecutionPlan')\n" +
                "@sink(type ='hl7', " +
                "uri = 'localhost:5091', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\"))) " +
                "define stream hl7sinkStream(payload string);";
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        InputHandler stream = executionPlanRuntime.getInputHandler("hl7sinkStream");
        executionPlanRuntime.start();
        String payLoadER71 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^B01|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|B01|20010101000000\r";
        String payLoadER72 = "MSH|^~\\&|SENDSYS|SENDFACILITY|TESTSYS|TESTFACILITY|20190123062351||ADT^P01|" +
                "M123768789T123456789X123456|P|2.3\r";
        stream.send(new Object[]{payLoadER71});
        stream.send(new Object[]{payLoadER72});
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        AssertJUnit.assertTrue(appender.getMessages().contains("Some error occurred while process the message." +
                " Error message:"));
        executionPlanRuntime.shutdown();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void hl7ConsumerTestInvalidHl7Encoding() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source to test invalid hl7 encoding");
        log.info("---------------------------------------------------------------------------------------------");
        receivedEvent = new ArrayList<>(3);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source( type = 'hl7',\n" +
                "port = '1040',\n" +
                "hl7.encoding = 'ER',\n" +
                "@map(type = 'text'))\n" +
                "define stream hl7stream (payload string);\n";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void hl7ConsumerTestInvalidAckEncoding() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source  to test invalid ack encoding");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Source.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        receivedEvent = new ArrayList<>(3);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source( type = 'hl7',\n" +
                "port = '5042',\n" +
                "hl7.encoding = 'ER7',\n" +
                "hl7.ack.encoding = 'ER',\n" +
                "@map(type = 'text'))\n" +
                "define stream hl7stream (payload string);\n";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void hl7ConsumerTestForConformanceProfileUsedWithNoErrors() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source to test to use Conformance Profile - Validation Success");
        log.info("---------------------------------------------------------------------------------------------");
        receivedEvent = new ArrayList<>(1);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source( type = 'hl7',\n" +
                "port = '5047',\n" +
                "hl7.encoding = 'ER7',\n" +
                "hl7.conformance.profile.used = 'true',\n" +
                "hl7.conformance.profile.file.path = 'src/test/resources/security/ADT_A01Msg.xml',\n" +
                "@map(type = 'text'))\n" +
                "define stream hl7stream (payload string);\n";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.addCallback("hl7stream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                for (Event event : events) {
                    eventArrived = true;
                    count.incrementAndGet();
                    log.info(event.toString().replaceAll("\r", "\n"));
                    try {
                        Message message = pipeParser.parse(event.getData(0).toString());
                        receivedEvent.add(testUtil.getControlID(message));
                    } catch (HL7Exception e) {
                        log.error(e);
                    }
                }
            }
        });
        String siddhiApp1 = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5047', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7sinkStream(payload string);";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        InputHandler stream = executionPlanRuntime.getInputHandler("hl7sinkStream");
        executionPlanRuntime.start();
        String payLoadER7 = "MSH|^~\\&|SEN|FAC|REC|FAC|||ADT^A01^ADT_A01|934576120110613083|P|2.3||||\r" +
                "EVN|A01||||\r";
        List<String> expected = new ArrayList<>(1);
        stream.send(new Object[]{payLoadER7});
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER7)));
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        AssertJUnit.assertEquals(1, count.get());
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(expected, receivedEvent);
        siddhiAppRuntime.shutdown();
        executionPlanRuntime.shutdown();
    }

    @Test
    public void hl7ConsumerTestForConformanceProfileUsedWithErrors() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source to test to use Conformance Profile - Validation Failure");
        log.info("---------------------------------------------------------------------------------------------");
        receivedEvent = new ArrayList<>(1);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source( type = 'hl7',\n" +
                "port = '5040',\n" +
                "hl7.encoding = 'ER7',\n" +
                "hl7.conformance.profile.used = 'true',\n" +
                "hl7.conformance.profile.file.path = 'src/test/resources/security/ADT_A01.xml',\n" +
                "@map(type = 'text'))\n" +
                "define stream hl7stream (payload string);\n";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.addCallback("hl7stream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                for (Event event : events) {
                    eventArrived = true;
                    count.incrementAndGet();
                    log.info(event.toString().replaceAll("\r", "\n"));
                    try {
                        Message message = pipeParser.parse(event.getData(0).toString());
                        receivedEvent.add(testUtil.getControlID(message));
                    } catch (HL7Exception e) {
                        log.error(e);
                    }
                }
            }
        });
        String siddhiApp1 = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5040', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7sinkStream(payload string);";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        InputHandler stream = executionPlanRuntime.getInputHandler("hl7sinkStream");
        executionPlanRuntime.start();
        String payLoadER7 = "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|" +
                "RECEIVING_FACILITY|20110613083617||ADT^A01|934576120110613083617|P|2.3||||\r" +
                "EVN|A01|20110613083617|||\r" +
                "PID|1||135769||MOUSE^MICKEY^||19281118|M|||123 Main St.^^Lake Buena Vista^FL^32830||(407)939-1289^" +
                "^^theMainMouse@disney.com|||||1719|99999999||||||||||||||||||||\r" +
                "PV1|1|O|||||^^^^^^^^|^^^^^^^^\r";
        List<String> expected = new ArrayList<>(1);
        stream.send(new Object[]{payLoadER7});
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER7)));
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        AssertJUnit.assertEquals(1, count.get());
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(expected, receivedEvent);
        siddhiAppRuntime.shutdown();
        executionPlanRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7ConsumerTestForConformanceFileWithEmptyField() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source to test with using conformance profile without giving file path");
        log.info("---------------------------------------------------------------------------------------------");
        receivedEvent = new ArrayList<>(1);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source( type = 'hl7',\n" +
                "port = '5040',\n" +
                "hl7.encoding = 'ER7',\n" +
                "hl7.conformance.profile.used = 'true',\n" +
                "@map(type = 'text'))\n" +
                "define stream hl7stream (payload string);\n";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.addCallback("hl7stream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                for (Event event : events) {
                    eventArrived = true;
                    count.incrementAndGet();
                    log.info(event.toString().replaceAll("\r", "\n"));
                    try {
                        Message message = pipeParser.parse(event.getData(0).toString());
                        receivedEvent.add(testUtil.getControlID(message));
                    } catch (HL7Exception e) {
                        log.error(e);
                    }
                }
            }
        });
        String siddhiApp1 = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5040', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7sinkStream(payload string);";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        InputHandler stream = executionPlanRuntime.getInputHandler("hl7sinkStream");
        executionPlanRuntime.start();
        String payLoadER7 = "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|" +
                "RECEIVING_FACILITY|20110613083617||ADT^A01|934576120110613083617|P|2.3||||\r" +
                "EVN|A01|20110613083617|||\r" +
                "PID|1||135769||MOUSE^MICKEY^||19281118|M|||123 Main St.^^Lake Buena Vista^FL^32830||(407)939-1289^" +
                "^^theMainMouse@disney.com|||||1719|99999999||||||||||||||||||||\r" +
                "PV1|1|O|||||^^^^^^^^|^^^^^^^^\r";
        List<String> expected = new ArrayList<>(1);
        stream.send(new Object[]{payLoadER7});
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER7)));
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        AssertJUnit.assertEquals(1, count.get());
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(expected, receivedEvent);
        siddhiAppRuntime.shutdown();
        executionPlanRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7ConsumerTestConformanceProfileInvalidFile() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test with the give conformance profile file is invalid. ");
        log.info("---------------------------------------------------------------------------------------------");
        receivedEvent = new ArrayList<>(1);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source( type = 'hl7',\n" +
                "port = '5041',\n" +
                "hl7.encoding = 'ER7',\n" +
                "hl7.conformance.profile.used = 'true',\n" +
                "hl7.conformance.profile.file.path = 'src/test/resources/security/kk.js',\n" +

                "@map(type = 'text'))\n" +
                "define stream hl7stream (payload string);\n";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.addCallback("hl7stream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                for (Event event : events) {
                    eventArrived = true;
                    count.incrementAndGet();
                    log.info(event.toString().replaceAll("\r", "\n"));
                    try {
                        Message message = pipeParser.parse(event.getData(0).toString());
                        receivedEvent.add(testUtil.getControlID(message));
                    } catch (HL7Exception e) {
                        log.error(e);
                    }
                }
            }
        });
        String siddhiApp1 = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5041', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7sinkStream(payload string);";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        InputHandler stream = executionPlanRuntime.getInputHandler("hl7sinkStream");
        executionPlanRuntime.start();
        String payLoadER7 = "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|" +
                "RECEIVING_FACILITY|20110613083617||ADT^A01|934576120110613083617|P|2.3||||\r" +
                "EVN|A01|20110613083617|||\r" +
                "PID|1||135769||MOUSE^MICKEY^||19281118|M|||123 Main St.^^Lake Buena Vista^FL^32830||(407)939-1289^" +
                "^^theMainMouse@disney.com|||||1719|99999999||||||||||||||||||||\r" +
                "PV1|1|O|||||^^^^^^^^|^^^^^^^^\r";
        List<String> expected = new ArrayList<>(1);
        stream.send(new Object[]{payLoadER7});
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER7)));
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        AssertJUnit.assertEquals(1, count.get());
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(expected, receivedEvent);
        siddhiAppRuntime.shutdown();
        executionPlanRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7ConsumerTestConformanceProfileFileNotFound() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test with the give conformance profile path is not found.");
        log.info("---------------------------------------------------------------------------------------------");
        receivedEvent = new ArrayList<>(1);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source( type = 'hl7',\n" +
                "port = '5041',\n" +
                "hl7.encoding = 'ER7',\n" +
                "hl7.conformance.profile.used = 'true',\n" +
                "hl7.conformance.profile.file.path = 'src/test',\n" +

                "@map(type = 'text'))\n" +
                "define stream hl7stream (payload string);\n";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.addCallback("hl7stream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                for (Event event : events) {
                    eventArrived = true;
                    count.incrementAndGet();
                    log.info(event.toString().replaceAll("\r", "\n"));
                    try {
                        Message message = pipeParser.parse(event.getData(0).toString());
                        receivedEvent.add(testUtil.getControlID(message));
                    } catch (HL7Exception e) {
                        log.error(e);
                    }
                }
            }
        });
        String siddhiApp1 = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5041', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7sinkStream(payload string);";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        InputHandler stream = executionPlanRuntime.getInputHandler("hl7sinkStream");
        executionPlanRuntime.start();
        String payLoadER7 = "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|" +
                "RECEIVING_FACILITY|20110613083617||ADT^A01|934576120110613083617|P|2.3||||\r" +
                "EVN|A01|20110613083617|||\r" +
                "PID|1||135769||MOUSE^MICKEY^||19281118|M|||123 Main St.^^Lake Buena Vista^FL^32830||(407)939-1289^" +
                "^^theMainMouse@disney.com|||||1719|99999999||||||||||||||||||||\r" +
                "PV1|1|O|||||^^^^^^^^|^^^^^^^^\r";
        List<String> expected = new ArrayList<>(1);
        stream.send(new Object[]{payLoadER7});
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER7)));
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        AssertJUnit.assertEquals(1, count.get());
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(expected, receivedEvent);
        siddhiAppRuntime.shutdown();
        executionPlanRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7ConsumerTestConformanceProfileWithProfileException() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source to test with conformance profile exception");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Hl7ReceivingApp.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        receivedEvent = new ArrayList<>(1);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source( type = 'hl7',\n" +
                "port = '5042',\n" +
                "hl7.encoding = 'ER7',\n" +
                "hl7.conformance.profile.used = 'true',\n" +
                "hl7.conformance.profile.file.path = 'src/test/resources/security/ADT_A31.xml',\n" +
                "@map(type = 'text'))\n" +
                "define stream hl7stream (payload string);\n";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.addCallback("hl7stream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                for (Event event : events) {
                    eventArrived = true;
                    count.incrementAndGet();
                    log.info(event.toString().replaceAll("\r", "\n"));
                    try {
                        Message message = pipeParser.parse(event.getData(0).toString());
                        receivedEvent.add(testUtil.getControlID(message));
                    } catch (HL7Exception e) {
                        log.error(e);
                    }
                }
            }
        });
        String siddhiApp1 = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5042', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7sinkStream(payload string);";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        InputHandler stream = executionPlanRuntime.getInputHandler("hl7sinkStream");
        executionPlanRuntime.start();
        String payLoadER7 = "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|" +
                "RECEIVING_FACILITY|20110613083617||ADT^A01|934576120110613083617|P|2.3||||\r" +
                "EVN|A01|20110613083617|||\r" +
                "PID|1||135769||MOUSE^MICKEY^||19281118|M|||123 Main St.^^Lake Buena Vista^FL^32830||(407)939-1289^" +
                "^^theMainMouse@disney.com|||||1719|99999999||||||||||||||||||||\r" +
                "PV1|1|O|||||^^^^^^^^|^^^^^^^^\r";
        List<String> expected = new ArrayList<>(1);
        stream.send(new Object[]{payLoadER7});
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER7)));
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        AssertJUnit.assertEquals(1, count.get());
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(expected, receivedEvent);
        AssertJUnit.assertTrue(appender.getMessages().contains("The given conformance Profile file is not" +
                " supported. Hence, dropping the validation."));
        siddhiAppRuntime.shutdown();
        executionPlanRuntime.shutdown();
    }

    @Test
    public void hl7ConsumerTestPauseAndResume() throws InterruptedException, HL7Exception {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source to test for pause and resume");
        log.info("---------------------------------------------------------------------------------------------");

        receivedEvent = new ArrayList<>(3);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source( type = 'hl7',\n" +
                "port = '5041',\n" +
                "hl7.encoding = 'ER7',\n" +
                "@map(type = 'text'))\n" +
                "define stream hl7stream (payload string);\n";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        Collection<List<Source>> sources = siddhiAppRuntime.getSources();
        siddhiAppRuntime.addCallback("hl7stream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {

                for (Event event : events) {
                    eventArrived = true;
                    count.incrementAndGet();
                    log.info(event.toString().replaceAll("\r", "\n"));
                    try {
                        Message message = pipeParser.parse(event.getData(0).toString());
                        receivedEvent.add(testUtil.getControlID(message));
                    } catch (HL7Exception e) {

                        log.error(e);
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        String siddhiApp1 = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5041', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7sinkStream(payload string);";
        List<String> expected = new ArrayList<>(3);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp1);
        InputHandler stream = executionPlanRuntime.getInputHandler("hl7sinkStream");
        executionPlanRuntime.start();

        String payLoadER71 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A04|" +
                "Q123456789T123456789X123456|P|2.3\r";
        String payLoadER72 = "MSH|^~\\&|||||20190122111442.228+0530||ORM^O01|6101|T|2.3\r";
        String payLoadER73 = "MSH|^~\\&|||||20190123062351.436+0530||ORU^R01^ORU_R01|6401|T|2.4\r";

        sources.forEach(e -> e.forEach(Source::pause));
        log.info("SiddhiApp paused...............................");

        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        Assert.assertFalse(eventArrived);
        stream.send(new Object[]{payLoadER71});
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);

        sources.forEach(e -> e.forEach(Source::resume));
        log.info("SiddhiApp resumed.............................");
        stream.send(new Object[]{payLoadER72});
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);

        stream.send(new Object[]{payLoadER73});
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);

        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER71)));
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER72)));
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER73)));
        AssertJUnit.assertEquals(3, count.get());
        AssertJUnit.assertEquals(expected, receivedEvent);
        siddhiAppRuntime.shutdown();
        executionPlanRuntime.shutdown();
    }
}
