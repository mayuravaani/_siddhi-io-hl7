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
import ca.uhn.hl7v2.parser.PipeParser;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.hl7.util.TestUtil;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCaseOfHl7SourceForTls {

    private static Logger log = Logger.getLogger(TestCaseOfHl7SourceForTls.class);
    private AtomicInteger count = new AtomicInteger();
    private volatile boolean eventArrived;
    private List<String> receivedEvent;
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
    public void hl7ConsumerTestForTlsEnabled() throws InterruptedException, HL7Exception {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test for enabling tls - System uses default keystore");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source ( type = 'hl7',\n" +
                "port = '4000',\n" +
                "hl7.encoding = 'xml',\n" +
                "tls.enabled = 'true',\n" +
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
                "uri = 'localhost:4000', " +
                "hl7.encoding = 'er7', " +
                "tls.enabled = 'true', " +
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
        Thread.sleep(10000);
        List<String> expected = new ArrayList<>(2);
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER71)));
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER72)));
        AssertJUnit.assertEquals(2, count.get());
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(expected, receivedEvent);
        siddhiAppRuntime.shutdown();
        executionPlanRuntime.shutdown();
    }

    @Test
    public void hl7ConsumerTlsPathDefinedByUser() throws InterruptedException, HL7Exception {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test for enabling tls - User gives path for his keystore file");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source ( type = 'hl7',\n" +
                "port = '5046',\n" +
                "hl7.encoding = 'xml',\n" +
                "tls.enabled = 'true',\n" +
                "tls.keystore.filepath = 'src/test/resources/keystore.jks', " +
                "tls.keystore.passphrase = 'changeit', " +
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
                "tls.enabled = 'true', " +
                "tls.keystore.filepath = 'src/test/resources/keystore.jks', " +
                "tls.keystore.passphrase = 'changeit', " +
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
        Thread.sleep(10000);
        List<String> expected = new ArrayList<>(2);
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER71)));
        expected.add(testUtil.getControlID(pipeParser.parse(payLoadER72)));
        AssertJUnit.assertEquals(2, count.get());
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(expected, receivedEvent);
        siddhiAppRuntime.shutdown();
        executionPlanRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7ConsumerTestForInvalidKeystoreFile() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test for enabling tls - Invalid keystore file");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source ( type = 'hl7',\n" +
                "port = '5046',\n" +
                "hl7.encoding = 'xml',\n" +
                "tls.enabled = 'true',\n" +
                "tls.keystore.filepath = 'src/test/resources/security/invalid.jks', " +
                "tls.keystore.passphrase = 'wso2carbon', " +
                "@map (type = 'xml', namespaces='ns=urn:hl7-org:v2xml', @attributes(MSH10 = \"ns:MSH/ns:MSH.10\"," +
                "MSH3HD1 = \"ns:MSH/ns:MSH.3/ns:HD.1\")))\n" +
                "define stream hl7stream (MSH10 string, MSH3HD1 string);\n";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7ConsumerTestForInvalidKeystorePath() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test for enabling tls - given path is invalid");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source ( type = 'hl7',\n" +
                "port = '5046',\n" +
                "hl7.encoding = 'xml',\n" +
                "tls.enabled = 'true',\n" +
                "tls.keystore.filepath = 'src/keystore.jks', " +
                "tls.keystore.passphrase = 'changeit', " +
                "@map (type = 'xml', namespaces='ns=urn:hl7-org:v2xml', @attributes(MSH10 = \"ns:MSH/ns:MSH.10\"," +
                "MSH3HD1 = \"ns:MSH/ns:MSH.3/ns:HD.1\")))\n" +
                "define stream hl7stream (MSH10 string, MSH3HD1 string);\n";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7ConsumerTestForWrongPassword() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test for enabling tls - given passphrase is incorrect");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source ( type = 'hl7',\n" +
                "port = '5046',\n" +
                "hl7.encoding = 'xml',\n" +
                "tls.enabled = 'true',\n" +
                "tls.keystore.filepath = 'src/test/resources/security/keystore.jks', " +
                "tls.keystore.passphrase = 'changeitrr', " +
                "@map (type = 'xml', namespaces='ns=urn:hl7-org:v2xml', @attributes(MSH10 = \"ns:MSH/ns:MSH.10\"," +
                "MSH3HD1 = \"ns:MSH/ns:MSH.3/ns:HD.1\")))\n" +
                "define stream hl7stream (MSH10 string, MSH3HD1 string);\n";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7ConsumerTestForInvalidKeystoreType() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test for enabling tls - invalid keystore type");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source ( type = 'hl7',\n" +
                "port = '5046',\n" +
                "hl7.encoding = 'xml',\n" +
                "tls.enabled = 'true',\n" +
                "tls.keystore.type = 's', " +
                "tls.keystore.filepath = 'src/test/resources/security/keystore.jks', " +
                "tls.keystore.passphrase = 'changeit', " +
                "@map (type = 'xml', namespaces='ns=urn:hl7-org:v2xml', @attributes(MSH10 = \"ns:MSH/ns:MSH.10\"," +
                "MSH3HD1 = \"ns:MSH/ns:MSH.3/ns:HD.1\")))\n" +
                "define stream hl7stream (MSH10 string, MSH3HD1 string);\n";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7ConsumerTestForWrongFile() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test for enabling tls - invalid file");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@source ( type = 'hl7',\n" +
                "port = '5046',\n" +
                "hl7.encoding = 'xml',\n" +
                "tls.enabled = 'true',\n" +
                "tls.keystore.filepath = 'src/test/resources/security/kk.js', " +
                "tls.keystore.passphrase = 'changeit', " +
                "@map (type = 'xml', namespaces='ns=urn:hl7-org:v2xml', @attributes(MSH10 = \"ns:MSH/ns:MSH.10\"," +
                "MSH3HD1 = \"ns:MSH/ns:MSH.3/ns:HD.1\")))\n" +
                "define stream hl7stream (MSH10 string, MSH3HD1 string);\n";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }
}
