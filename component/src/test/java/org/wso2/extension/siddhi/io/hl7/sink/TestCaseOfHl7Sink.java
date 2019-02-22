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
package org.wso2.extension.siddhi.io.hl7.sink;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.DefaultXMLParser;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.parser.XMLParser;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.hl7.util.TestUtil;
import org.wso2.extension.siddhi.io.hl7.util.UnitTestAppender;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.io.File;
import java.nio.charset.UnsupportedCharsetException;

public class TestCaseOfHl7Sink {

    private static Logger log = Logger.getLogger(TestCaseOfHl7Sink.class);
    private volatile int count;
    private volatile boolean eventArrived;
    private Hl7SinkTestUtil hl7SinkTestUtil;
    private PipeParser pipeParser = new PipeParser();
    private XMLParser xmlParser = new DefaultXMLParser();
    private TestUtil testUtil = new TestUtil();

    @BeforeMethod
    public void initBeforeMethod() {

        count = 0;
        eventArrived = false;
        hl7SinkTestUtil = new Hl7SinkTestUtil();
        File keyStoreFilePath = new File("src/test");
        String keyStorePath = keyStoreFilePath.getAbsolutePath();
        System.setProperty("carbon.home", keyStorePath);
    }

    @Test
    public void hl7PublishTestER7() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with ER7 format message - multiple messages and different Message Types.");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5011', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        hl7SinkTestUtil.connect(5011, count, eventArrived, false, 3);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        siddhiAppRuntime.start();
        String payLoadER71 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A01|" +
                "M123456789T123456789X123456|P|2.3\r" +
                "EVN|A01|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r" +
                "PID|1||123456789|0123456789^AA^^JP|BROS^MARIO^^^^||19850101000000|M|||" +
                "123 FAKE STREET^MARIO \\T\\ LUIGI BROS PLACE^TOADSTOOL KINGDOM^NES^A1B2C3^JP^HOME^^1234|1234|" +
                "(555)555-0123^HOME^JP:1234567|||S|MSH|12345678|||||||0|||||N\r" +
                "NK1|1|PEACH^PRINCESS^^^^|SO|ANOTHER CASTLE^^TOADSTOOL KINGDOM^NES^^JP|(123)555-1234|" +
                "(123)555-2345|NOK|||||||||||||\r" +
                "NK2|2|TOADSTOOL^PRINCESS^^^^|SO|YET ANOTHER CASTLE^^TOADSTOOL KINGDOM^NES^^JP|(123)555-3456|" +
                "(123)555-4567|EMC|||||||||||||\r" +
                "PV1|1|O|ABCD^EFGH^|||^^|123456^DINO^YOSHI^^^^^^MSRM^CURRENT^^^NEIGHBOURHOOD DR NBR^|" +
                "^DOG^DUCKHUNT^^^^^^^CURRENT||CRD|||||||123456^DINO^YOSHI^^^^^^MSRM^CURRENT^^^NEIGHBOURHOOD " +
                "DR NBR^|AO|0123456789|1|||||||||||||||||||MSH||A|||20010101000000\r" +
                "IN1|1|PAR^PARENT||||LUIGI\r" +
                "IN2|2|FRI^FRIEND||||PRINCESS\r";
        String payLoadER72 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A04|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A04|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r";
        String payLoadER73 = "MSH|^~\\&|||||20190122111442.228+0530||ORU^R01|R6546556101|T|2.3\r";
        try {
            stream.send(new Object[]{payLoadER71});
            stream.send(new Object[]{payLoadER72});
            stream.send(new Object[]{payLoadER73});

        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }
        Message payLoadER71Msg = pipeParser.parse(payLoadER71);
        Message payLoadER72Msg = pipeParser.parse(payLoadER72);
        Message payLoadER73Msg = pipeParser.parse(payLoadER73);
        Thread.sleep(3000);
        count = hl7SinkTestUtil.getCount();
        eventArrived = hl7SinkTestUtil.getEventArrived();
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadER71Msg)));
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadER72Msg)));
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadER73Msg)));
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void hl7PublishTestXMLADT() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with XML format message example for ADT (Admission,Discharge,Transfer) message Type.");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5011', " +
                "hl7.encoding = 'xml', " +
                "@map(type = 'xml', enclosing.element=\"<ADT_A01  xmlns='urn:hl7-org:v2xml'>\", " +
                "@payload('<MSH><MSH.1>{{MSH1}}</MSH.1><MSH.2>{{MSH2}}</MSH.2><MSH.3><HD.1>{{MSH3HD1}}</HD.1>" +
                "</MSH.3><MSH.4><HD.1>{{MSH4HD1}}</HD.1></MSH.4><MSH.5><HD.1>{{MSH5HD1}}</HD.1></MSH.5><MSH.6>" +
                "<HD.1>{{MSH6HD1}}</HD.1></MSH.6><MSH.7>{{MSH7}}</MSH.7><MSH.8>{{MSH8}}</MSH.8><MSH.9><CM_MSG.1>" +
                "{{CM_MSG1}}</CM_MSG.1><CM_MSG.2>{{CM_MSG2}}</CM_MSG.2></MSH.9><MSH.10>{{MSH10}}</MSH.10><MSH.11>" +
                "{{MSH11}}</MSH.11><MSH.12>{{MSH12}}</MSH.12></MSH>'))) " +
                "define stream hl7stream(MSH1 string,MSH2 string,MSH3HD1 string,MSH4HD1 string,MSH5HD1 string," +
                "MSH6HD1 string,MSH7 string,MSH8 string,CM_MSG1 string,CM_MSG2 string,MSH10 string,MSH11 string," +
                "MSH12 string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        hl7SinkTestUtil.connect(5011, count, eventArrived, false, 2);
        siddhiAppRuntime.start();
        try {
            stream.send(new Object[]{"|", "^~\\&amp;", "sendingSystemA", "senderFacilityA", "receivingSystemA",
                    "receivingFacilityA", "20080925161613", " ", "ADT", "A01", "S123456789", "P", "2.3"});
            stream.send(new Object[]{"|", "^~\\&amp;", "sendingSystemB", "senderFacilityB", "receivingSystemB",
                    "receivingFacilityB", "20080925161613", " ", "ADT", "A01", "T123456789", "P", "2.3"});
        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }
        String payLoadXML1 = "<ADT_A01  xmlns='urn:hl7-org:v2xml'><MSH><MSH.1>|</MSH.1><MSH.2>^~\\&amp;</MSH.2>" +
                "<MSH.3><HD.1>sendingSystem</HD.1></MSH.3><MSH.4><HD.1>senderFacility</HD.1></MSH.4><MSH.5><HD.1>" +
                "receivingSystem</HD.1></MSH.5><MSH.6><HD.1>receivingFacility</HD.1></MSH.6><MSH.7>20080925161613" +
                "</MSH.7><MSH.8></MSH.8><MSH.9><CM_MSG.1>ADT</CM_MSG.1><CM_MSG.2>A01</CM_MSG.2></MSH.9><MSH.10>" +
                "S123456789</MSH.10><MSH.11>P</MSH.11><MSH.12>2.3</MSH.12></MSH></ADT_A01>";
        String payLoadXML2 = "<ADT_A01  xmlns='urn:hl7-org:v2xml'><MSH><MSH.1>|</MSH.1><MSH.2>^~\\&amp;</MSH.2>" +
                "<MSH.3><HD.1>sendingSystem</HD.1></MSH.3><MSH.4><HD.1>senderFacility</HD.1></MSH.4><MSH.5><HD.1>" +
                "receivingSystem</HD.1></MSH.5><MSH.6><HD.1>receivingFacility</HD.1></MSH.6><MSH.7>20080925161613" +
                "</MSH.7><MSH.8></MSH.8><MSH.9><CM_MSG.1>ADT</CM_MSG.1><CM_MSG.2>A01</CM_MSG.2></MSH.9><MSH.10>" +
                "S123456789</MSH.10><MSH.11>P</MSH.11><MSH.12>2.3</MSH.12></MSH></ADT_A01>";
        Message payLoadXML1Msg = xmlParser.parse(payLoadXML1);
        Message payLoadXML2Msg = xmlParser.parse(payLoadXML2);
        Thread.sleep(3000);
        count = hl7SinkTestUtil.getCount();
        eventArrived = hl7SinkTestUtil.getEventArrived();
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadXML1Msg)));
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadXML2Msg)));
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void hl7PublishTestXMLORM() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with XML format message example for ORM (Order Message) message Type.");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5011', " +
                "hl7.encoding = 'xml', " +
                "@map(type = 'xml', enclosing.element=\"<ORM_O01  xmlns='urn:hl7-org:v2xml'>\", " +
                "@payload('<MSH><MSH.1>{{MSH1}}</MSH.1><MSH.2>{{MSH2}}</MSH.2><MSH.7><TS.1>{{MSH7TS1}}</TS.1>" +
                "</MSH.7><MSH.9><CM_MSG.1>{{MSH9CM_MSG1}}</CM_MSG.1><CM_MSG.2>{{MSH9CM_MSG2}}</CM_MSG.2></MSH.9>" +
                "<MSH.10>{{MSH10}}</MSH.10><MSH.11><PT.1>{{MSH11PT1}}</PT.1></MSH.11><MSH.12>{{MSH12}}" +
                "</MSH.12></MSH>'))) " +
                "define stream hl7stream(MSH1 string,MSH2 string,MSH7TS1 string,MSH9CM_MSG1 string," +
                "MSH9CM_MSG2 string,MSH10 string,MSH11PT1 string,MSH12 string);  ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        hl7SinkTestUtil.connect(5011, count, eventArrived, false, 1);
        siddhiAppRuntime.start();
        try {
            stream.send(new Object[]{"|", "^~\\&amp;", "20190122111442.228+0530", "ORM", "O01", "Q2336101",
                    "T", "2.3"});
        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }
        String payLoadXML = "<ORM_O01  xmlns='urn:hl7-org:v2xml'><MSH><MSH.1>|</MSH.1><MSH.2>^~\\&amp;</MSH.2>" +
                "<MSH.7><TS.1>20190122111442.228+0530</TS.1></MSH.7><MSH.9><CM_MSG.1>ORM</CM_MSG.1>" +
                "<CM_MSG.2>O01</CM_MSG.2></MSH.9><MSH.10>Q2336101</MSH.10><MSH.11><PT.1>T" +
                "</PT.1></MSH.11><MSH.12>2.3</MSH.12></MSH></ORM_O01>";
        Message payLoadXMLMsg = xmlParser.parse(payLoadXML);
        Thread.sleep(3000);
        count = hl7SinkTestUtil.getCount();
        eventArrived = hl7SinkTestUtil.getEventArrived();
        AssertJUnit.assertEquals(1, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadXMLMsg)));
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void hl7PublishTestUriFormatWithScheme() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with uri format hl7://<host>:<port>.");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5011', " +
                "hl7.encoding = 'xml', " +
                "@map(type = 'xml', enclosing.element=\"<ORM_O01  xmlns='urn:hl7-org:v2xml'>\", " +
                "@payload('<MSH><MSH.1>{{MSH1}}</MSH.1><MSH.2>{{MSH2}}</MSH.2><MSH.7><TS.1>{{MSH7TS1}}</TS.1>" +
                "</MSH.7><MSH.9><CM_MSG.1>{{MSH9CM_MSG1}}</CM_MSG.1><CM_MSG.2>{{MSH9CM_MSG2}}</CM_MSG.2></MSH.9>" +
                "<MSH.10>{{MSH10}}</MSH.10><MSH.11><PT.1>{{MSH11PT1}}</PT.1></MSH.11><MSH.12>{{MSH12}}" +
                "</MSH.12></MSH>'))) " +
                "define stream hl7stream(MSH1 string,MSH2 string,MSH7TS1 string,MSH9CM_MSG1 string," +
                "MSH9CM_MSG2 string,MSH10 string,MSH11PT1 string,MSH12 string);  ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        hl7SinkTestUtil.connect(5011, count, eventArrived, false, 1);
        siddhiAppRuntime.start();
        try {
            stream.send(new Object[]{"|", "^~\\&amp;", "20190122111442.228+0530", "ORM", "O01", "Q2336101",
                    "T", "2.3"});
        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }
        String payLoadXML = "<ORM_O01  xmlns='urn:hl7-org:v2xml'><MSH><MSH.1>|</MSH.1><MSH.2>^~\\&amp;</MSH.2>" +
                "<MSH.7><TS.1>20190122111442.228+0530</TS.1></MSH.7><MSH.9><CM_MSG.1>ORM</CM_MSG.1>" +
                "<CM_MSG.2>O01</CM_MSG.2></MSH.9><MSH.10>Q2336101</MSH.10><MSH.11><PT.1>T" +
                "</PT.1></MSH.11><MSH.12>2.3</MSH.12></MSH></ORM_O01>";
        Message payLoadXMLMsg = xmlParser.parse(payLoadXML);
        Thread.sleep(3000);
        count = hl7SinkTestUtil.getCount();
        eventArrived = hl7SinkTestUtil.getEventArrived();
        AssertJUnit.assertEquals(1, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadXMLMsg)));
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void hl7PublishTestInvalidUriFormat() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with uri format hl7://<host>:<port>.");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://:localhost:5011', " +
                "hl7.encoding = 'xml', " +
                "@map(type = 'xml', enclosing.element=\"<ORM_O01  xmlns='urn:hl7-org:v2xml'>\", " +
                "@payload('<MSH><MSH.1>{{MSH1}}</MSH.1><MSH.2>{{MSH2}}</MSH.2><MSH.7><TS.1>{{MSH7TS1}}</TS.1>" +
                "</MSH.7><MSH.9><CM_MSG.1>{{MSH9CM_MSG1}}</CM_MSG.1><CM_MSG.2>{{MSH9CM_MSG2}}</CM_MSG.2></MSH.9>" +
                "<MSH.10>{{MSH10}}</MSH.10><MSH.11><PT.1>{{MSH11PT1}}</PT.1></MSH.11><MSH.12>{{MSH12}}" +
                "</MSH.12></MSH>'))) " +
                "define stream hl7stream(MSH1 string,MSH2 string,MSH7TS1 string,MSH9CM_MSG1 string," +
                "MSH9CM_MSG2 string,MSH10 string,MSH11PT1 string,MSH12 string);  ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        hl7SinkTestUtil.connect(5011, count, eventArrived, false, 1);
        siddhiAppRuntime.start();
        try {
            stream.send(new Object[]{"|", "^~\\&amp;", "20190122111442.228+0530", "ORM", "O01", "Q2336101",
                    "T", "2.3"});
        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }
        String payLoadXML = "<ORM_O01  xmlns='urn:hl7-org:v2xml'><MSH><MSH.1>|</MSH.1><MSH.2>^~\\&amp;</MSH.2>" +
                "<MSH.7><TS.1>20190122111442.228+0530</TS.1></MSH.7><MSH.9><CM_MSG.1>ORM</CM_MSG.1>" +
                "<CM_MSG.2>O01</CM_MSG.2></MSH.9><MSH.10>Q2336101</MSH.10><MSH.11><PT.1>T" +
                "</PT.1></MSH.11><MSH.12>2.3</MSH.12></MSH></ORM_O01>";
        Message payLoadXMLMsg = xmlParser.parse(payLoadXML);
        Thread.sleep(3000);
        count = hl7SinkTestUtil.getCount();
        eventArrived = hl7SinkTestUtil.getEventArrived();
        AssertJUnit.assertEquals(1, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadXMLMsg)));
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void hl7PublishTestInvalidUri() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with uri format for Invalid Uri hl7://<host>:<port>.");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Hl7Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5o11', " +
                "hl7.encoding = 'xml', " +
                "@map(type = 'xml', enclosing.element=\"<ORM_O01  xmlns='urn:hl7-org:v2xml'>\", " +
                "@payload('<MSH><MSH.1>{{MSH1}}</MSH.1><MSH.2>{{MSH2}}</MSH.2><MSH.7><TS.1>{{MSH7TS1}}</TS.1>" +
                "</MSH.7><MSH.9><CM_MSG.1>{{MSH9CM_MSG1}}</CM_MSG.1><CM_MSG.2>{{MSH9CM_MSG2}}</CM_MSG.2></MSH.9>" +
                "<MSH.10>{{MSH10}}</MSH.10><MSH.11><PT.1>{{MSH11PT1}}</PT.1></MSH.11><MSH.12>{{MSH12}}" +
                "</MSH.12></MSH>'))) " +
                "define stream hl7stream(MSH1 string,MSH2 string,MSH7TS1 string,MSH9CM_MSG1 string," +
                "MSH9CM_MSG2 string,MSH10 string,MSH11PT1 string,MSH12 string);  ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void hl7PublishTestInvalidUriForm() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with uri format for Invalid Uri hl7://<host>:<port>.");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Hl7Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://local<host:5o11', " +
                "hl7.encoding = 'xml', " +
                "@map(type = 'xml', enclosing.element=\"<ORM_O01  xmlns='urn:hl7-org:v2xml'>\", " +
                "@payload('<MSH><MSH.1>{{MSH1}}</MSH.1><MSH.2>{{MSH2}}</MSH.2><MSH.7><TS.1>{{MSH7TS1}}</TS.1>" +
                "</MSH.7><MSH.9><CM_MSG.1>{{MSH9CM_MSG1}}</CM_MSG.1><CM_MSG.2>{{MSH9CM_MSG2}}</CM_MSG.2></MSH.9>" +
                "<MSH.10>{{MSH10}}</MSH.10><MSH.11><PT.1>{{MSH11PT1}}</PT.1></MSH.11><MSH.12>{{MSH12}}" +
                "</MSH.12></MSH>'))) " +
                "define stream hl7stream(MSH1 string,MSH2 string,MSH7TS1 string,MSH9CM_MSG1 string," +
                "MSH9CM_MSG2 string,MSH10 string,MSH11PT1 string,MSH12 string);  ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void hl7PublishTestUriInvalid() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with invalid Uri <host>:<port>.");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Hl7Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5o11', " +
                "hl7.encoding = 'xml', " +
                "@map(type = 'xml', enclosing.element=\"<ORM_O01  xmlns='urn:hl7-org:v2xml'>\", " +
                "@payload('<MSH><MSH.1>{{MSH1}}</MSH.1><MSH.2>{{MSH2}}</MSH.2><MSH.7><TS.1>{{MSH7TS1}}</TS.1>" +
                "</MSH.7><MSH.9><CM_MSG.1>{{MSH9CM_MSG1}}</CM_MSG.1><CM_MSG.2>{{MSH9CM_MSG2}}</CM_MSG.2></MSH.9>" +
                "<MSH.10>{{MSH10}}</MSH.10><MSH.11><PT.1>{{MSH11PT1}}</PT.1></MSH.11><MSH.12>{{MSH12}}" +
                "</MSH.12></MSH>'))) " +
                "define stream hl7stream(MSH1 string,MSH2 string,MSH7TS1 string,MSH9CM_MSG1 string," +
                "MSH9CM_MSG2 string,MSH10 string,MSH11PT1 string,MSH12 string);  ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void hl7PublishTestAckEncoding() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 sink test with logging format of hl7 Acknowledgment message encoding Type.");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5011', " +
                "hl7.encoding = 'er7', " +
                "hl7.ack.encoding = 'xml', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);  ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        hl7SinkTestUtil.connect(5011, count, eventArrived, false, 1);
        siddhiAppRuntime.start();
        String payLoadER7 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A04|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A04|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r";
        try {
            stream.send(new Object[]{payLoadER7});
        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }
        Message payLoadER7Msg = pipeParser.parse(payLoadER7);
        Thread.sleep(3000);
        count = hl7SinkTestUtil.getCount();
        eventArrived = hl7SinkTestUtil.getEventArrived();
        AssertJUnit.assertEquals(1, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadER7Msg)));
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void hl7PublishTestCharset() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with different charset Type.");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5011', " +
                "hl7.encoding = 'er7', " +
                "charset = 'ISO-8859-2', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        hl7SinkTestUtil.connect(5011, count, eventArrived, false, 1);
        siddhiAppRuntime.start();
        String payLoadER7 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A04|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A04|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r";
        try {
            stream.send(new Object[]{payLoadER7});

        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }

        Message payLoadER7Msg = pipeParser.parse(payLoadER7);
        Thread.sleep(3000);
        count = hl7SinkTestUtil.getCount();
        eventArrived = hl7SinkTestUtil.getEventArrived();
        AssertJUnit.assertEquals(1, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadER7Msg)));
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void hl7PublishTestMissingMandatoryProperty() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with missing mandatory parameters.");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5011', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiManager.shutdown();
    }

    @Test
    public void hl7PublishTestConnectionUnavailable() throws InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 sink test with a unavailable server.");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5015', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        Thread.sleep(10000);
        AssertJUnit.assertTrue(appender.getMessages().contains("Failed to connect with the HL7 server, check"));
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void hl7PublishTesInvalidUri() throws InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with Invalid Uri Scheme.");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'h-l7://localhost:5012', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        siddhiManager.createSiddhiAppRuntime(siddhiApp);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        Hl7SinkTestUtil hl7SinkTestUtil1 = new Hl7SinkTestUtil();
        hl7SinkTestUtil1.connect(5012, count, eventArrived, false, 2);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void hl7PublishTestUnSupportCharsetForServer() throws InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("Sink test if the client uses multiple-byte-character encoding & server uses single-byte encoding ");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Hl7Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5011', " +
                "hl7.encoding = 'er7', " +
                "charset = 'UTF-16', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        hl7SinkTestUtil.connect(5011, count, eventArrived, false, 1);
        siddhiAppRuntime.start();
        String payLoadER7 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A04|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A04|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r";
        try {
            stream.send(new Object[]{payLoadER7});
        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }
        AssertJUnit.assertTrue(appender.getMessages().contains("Error occurred while processing the message." +
                " Please check the TestExecutionPlan:hl7stream"));
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void hl7PublishTestWithDifferentTimeout() throws HL7Exception, InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with ER7 format message - multiple messages and different Message Types.");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5011', " +
                "hl7.encoding = 'er7', " +
                "hl7.timeout = '5000', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        hl7SinkTestUtil.connect(5011, count, eventArrived, false, 3);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        siddhiAppRuntime.start();
        String payLoadER71 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A01|" +
                "M123456789T123456789X123456|P|2.3\r" +
                "EVN|A01|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r" +
                "PID|1||123456789|0123456789^AA^^JP|BROS^MARIO^^^^||19850101000000|M|||" +
                "123 FAKE STREET^MARIO \\T\\ LUIGI BROS PLACE^TOADSTOOL KINGDOM^NES^A1B2C3^JP^HOME^^1234|1234|" +
                "(555)555-0123^HOME^JP:1234567|||S|MSH|12345678|||||||0|||||N\r" +
                "NK1|1|PEACH^PRINCESS^^^^|SO|ANOTHER CASTLE^^TOADSTOOL KINGDOM^NES^^JP|(123)555-1234|" +
                "(123)555-2345|NOK|||||||||||||\r" +
                "NK2|2|TOADSTOOL^PRINCESS^^^^|SO|YET ANOTHER CASTLE^^TOADSTOOL KINGDOM^NES^^JP|(123)555-3456|" +
                "(123)555-4567|EMC|||||||||||||\r" +
                "PV1|1|O|ABCD^EFGH^|||^^|123456^DINO^YOSHI^^^^^^MSRM^CURRENT^^^NEIGHBOURHOOD DR NBR^|" +
                "^DOG^DUCKHUNT^^^^^^^CURRENT||CRD|||||||123456^DINO^YOSHI^^^^^^MSRM^CURRENT^^^NEIGHBOURHOOD " +
                "DR NBR^|AO|0123456789|1|||||||||||||||||||MSH||A|||20010101000000\r" +
                "IN1|1|PAR^PARENT||||LUIGI\r" +
                "IN2|2|FRI^FRIEND||||PRINCESS\r";
        String payLoadER72 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A04|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A04|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r";
        String payLoadER73 = "MSH|^~\\&|||||20190122111442.228+0530||ORU^R01|R6546556101|T|2.3\r";
        try {
            stream.send(new Object[]{payLoadER71});
            stream.send(new Object[]{payLoadER72});
            stream.send(new Object[]{payLoadER73});

        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }
        Message payLoadER71Msg = pipeParser.parse(payLoadER71);
        Message payLoadER72Msg = pipeParser.parse(payLoadER72);
        Message payLoadER73Msg = pipeParser.parse(payLoadER73);
        Thread.sleep(3000);
        count = hl7SinkTestUtil.getCount();
        eventArrived = hl7SinkTestUtil.getEventArrived();
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadER71Msg)));
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadER72Msg)));
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadER73Msg)));
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = UnsupportedCharsetException.class)
    public void hl7PublishTestUnsupportedCharset() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with given charset Type is invalid.");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5011', " +
                "hl7.encoding = 'er7', " +
                "charset = 'UTF_8', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void hl7PublishTestUnsupportedHl7Encoding() throws InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with UnSupported Hl7 Encoding - giving text format and preferred as xml");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Hl7Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5011', " +
                "hl7.encoding = 'xml', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        hl7SinkTestUtil.connect(5011, count, eventArrived, false, 3);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        siddhiAppRuntime.start();
        String payLoadER7 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A04|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A04|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r";

        try {
            stream.send(new Object[]{payLoadER7});

        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }
        AssertJUnit.assertTrue(appender.getMessages().contains("Error occurred while processing the message. Please " +
                "check the TestExecutionPlan:hl7stream"));
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void hl7PublishTestInvalidHl7Encoding() throws InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with Invalid Hl7 Encoding -  given hl7 encoding format is invalid");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Hl7Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5011', " +
                "hl7.encoding = 'exml', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        hl7SinkTestUtil.connect(5011, count, eventArrived, false, 3);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        siddhiAppRuntime.start();
        String payLoadER7 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A04|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A04|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r";

        try {
            stream.send(new Object[]{payLoadER7});

        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void hl7PublishTestInvalidHl7AckEncoding() throws InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with Invalid Hl7 Encoding -  given hl7 encoding format is invalid");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Hl7Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5011', " +
                "hl7.encoding = 'exml', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        hl7SinkTestUtil.connect(5011, count, eventArrived, false, 3);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        siddhiAppRuntime.start();
        String payLoadER7 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A04|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A04|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r";
        try {
            stream.send(new Object[]{payLoadER7});

        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void hl7PublishTestInputMessageWrongFormat() throws InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with ER7 format message - Input message format is invalid.");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'localhost:5011', " +
                "hl7.encoding = 'er7', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        hl7SinkTestUtil.connect(5011, count, eventArrived, false, 3);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        siddhiAppRuntime.start();
        String payLoadER72 = "MSH|^~&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A04|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A04|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r";
        try {
            stream.send(new Object[]{payLoadER72});

        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }
        siddhiAppRuntime.shutdown();
    }

}
