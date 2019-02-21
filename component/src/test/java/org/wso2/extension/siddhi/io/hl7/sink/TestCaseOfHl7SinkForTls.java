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
import ca.uhn.hl7v2.parser.PipeParser;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.hl7.util.TestUtil;
import org.wso2.extension.siddhi.io.hl7.util.UnitTestAppender;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.File;

public class TestCaseOfHl7SinkForTls {

    private volatile int count;
    private volatile boolean eventArrived;
    private static Logger log = Logger.getLogger(TestCaseOfHl7SinkForTls.class);
    private PipeParser pipeParser = new PipeParser();
    private TestUtil testUtil = new TestUtil();
    private Hl7SinkTestUtil hl7SinkTestUtil;

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
    public void hl7PublishTestForTlsEnabled() throws InterruptedException, HL7Exception {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with tls Enabled");
        log.info("---------------------------------------------------------------------------------------------");

        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5019', " +
                "hl7.encoding = 'er7', " +
                "tls.enabled = 'true', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        hl7SinkTestUtil.connect(5019, count, eventArrived, true, 2);
        siddhiAppRuntime.start();
        String payLoadER71 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A04|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A04|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r";
        String payLoadER72 = "MSH|^~\\&|||||20190122111442.228+0530||ORU^R01|R6546556101|T|2.3\r";

        try {
            stream.send(new Object[]{payLoadER71});
            stream.send(new Object[]{payLoadER72});

        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }

        Message payLoadER71Msg = pipeParser.parse(payLoadER71);
        Message payLoadER72Msg = pipeParser.parse(payLoadER72);
        count = hl7SinkTestUtil.getCount();
        eventArrived = hl7SinkTestUtil.getEventArrived();
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadER71Msg)));
        AssertJUnit.assertTrue(hl7SinkTestUtil.assertMessageContent(testUtil.getControlID(payLoadER72Msg)));
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void hl7PublishTestForTlsSSLHandshakeException() throws InterruptedException {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with tls Enabled and tls of source not enabled");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Hl7Sink.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5011', " +
                "hl7.encoding = 'er7', " +
                "tls.enabled = 'true', " +
                "tls.keystore.filepath = 'src/test/resources/keystore.jks', " +
                "tls.keystore.passphrase = 'changeit', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        hl7SinkTestUtil.connect(5011, count, eventArrived, false, 1);
        siddhiAppRuntime.start();
        String payLoadER71 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A04|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A04|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r";
        try {
            stream.send(new Object[]{payLoadER71});

        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }
        AssertJUnit.assertTrue(appender.getMessages().contains("Interruption occurred while sending the message " +
                "from stream: TestExecutionPlan:hl7stream"));
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void hl7PublishTestForTlsPathDefinedByUser() throws InterruptedException, HL7Exception {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 Sink test with tls Enabled");
        log.info("---------------------------------------------------------------------------------------------");
        String tlsPath = "src/test/resources/keystore.jks";
        String passPhrase = "changeit";
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5014', " +
                "hl7.encoding = 'er7', " +
                "tls.enabled = 'true', " +
                "tls.keystore.filepath = 'src/test/resources/security/keystore.jks', " +
                "tls.keystore.passphrase = 'changeit', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        InputHandler stream = siddhiAppRuntime.getInputHandler("hl7stream");
        Hl7SinkTestUtil hl7SinkTestUtil1 = new Hl7SinkTestUtil();
        hl7SinkTestUtil1.connectWithTlsPath(5014, count, eventArrived, true, 2, tlsPath,
                passPhrase);
        siddhiAppRuntime.start();
        String payLoadER71 = "MSH|^~\\&|NES|NINTENDO|TESTSYSTEM|TESTFACILITY|20010101000000||ADT^A04|" +
                "Q123456789T123456789X123456|P|2.3\r" +
                "EVN|A04|20010101000000|||^KOOPA^BOWSER^^^^^^^CURRENT\r";
        String payLoadER72 = "MSH|^~\\&|||||20190122111442.228+0530||ORU^R01|R6546556101|T|2.3\r";

        try {
            stream.send(new Object[]{payLoadER71});
            stream.send(new Object[]{payLoadER72});

        } catch (InterruptedException e) {
            AssertJUnit.fail("interrupted");
        }

        Message payLoadER71Msg = pipeParser.parse(payLoadER71);
        Message payLoadER72Msg = pipeParser.parse(payLoadER72);
        count = hl7SinkTestUtil1.getCount();
        eventArrived = hl7SinkTestUtil1.getEventArrived();
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertTrue(hl7SinkTestUtil1.assertMessageContent(testUtil.getControlID(payLoadER71Msg)));
        AssertJUnit.assertTrue(hl7SinkTestUtil1.assertMessageContent(testUtil.getControlID(payLoadER72Msg)));
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7PublisherTestForInvalidKeystoreFile() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test for enabling tls - Invalid keystore file");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5014', " +
                "hl7.encoding = 'er7', " +
                "tls.enabled = 'true', " +
                "tls.keystore.filepath = 'src/test/resources/security/invalid.jks', " +
                "tls.keystore.passphrase = 'changeit', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7PublisherTestForInvalidKemystoreFile() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test for enabling tls - Invalid keystore file");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5014', " +
                "hl7.encoding = 'er7', " +
                "tls.enabled = 'true', " +
                "tls.keystore.filepath = 'src/test/resources/security/symmetrickey.jks', " +
                "tls.keystore.passphrase = 'trustpassword', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7PublisherTestForInvalidKeystorePath() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test for enabling tls - given path is invalid");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5014', " +
                "hl7.encoding = 'er7', " +
                "tls.enabled = 'true', " +
                "tls.keystore.filepath = 'src/keystore.jks', " +
                "tls.keystore.passphrase = 'changeit', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7PublisherTestForWrongPassword() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test for enabling tls - given passphrase is incorrect");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5014', " +
                "hl7.encoding = 'er7', " +
                "tls.enabled = 'true', " +
                "tls.keystore.filepath = 'src/test/resources/security/keystore.jks', " +
                "tls.keystore.passphrase = 'chaaangeit', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7PublisherTestForWrongFile() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test for enabling tls - invalid file");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5014', " +
                "hl7.encoding = 'er7', " +
                "tls.enabled = 'true', " +
                "tls.keystore.filepath = 'src/test/resources/security/kk.jks', " +
                "tls.keystore.passphrase = 'chaaangeit', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void hl7PublisherTestForWrongKeyStoreType() {

        log.info("---------------------------------------------------------------------------------------------");
        log.info("hl7 source test for enabling tls - invalid keystore type");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        String siddhiApp = "@App:name('TestExecutionPlan')\n" +
                "@sink(type='hl7', " +
                "uri = 'hl7://localhost:5014', " +
                "hl7.encoding = 'er7', " +
                "tls.enabled = 'true', " +
                "tls.keystore.type = 'vv', " +
                "tls.keystore.filepath = 'src/test/resources/security/keystore.jks', " +
                "tls.keystore.passphrase = 'changeit', " +
                "@map(type = 'text', @payload(\"{{payload}}\")))" +
                "define stream hl7stream(payload string);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

}
