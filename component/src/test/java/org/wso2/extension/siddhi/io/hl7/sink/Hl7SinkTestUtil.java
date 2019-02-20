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

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.app.HL7Service;
import ca.uhn.hl7v2.hoh.sockets.CustomCertificateTlsSocketFactory;
import ca.uhn.hl7v2.hoh.util.HapiSocketTlsFactoryWrapper;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.protocol.ApplicationRouter;
import ca.uhn.hl7v2.protocol.ReceivingApplication;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.hl7.util.TestUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Hl7SinkTestUtil {

    private static final Logger log = Logger.getLogger(Hl7SinkTestUtil.class);
    private HapiContext context = new DefaultHapiContext();
    private HL7Service hl7Service;
    private static int count;
    private static boolean eventArrived;
    private static List<String> results;
    private TestUtil testUtil = new TestUtil();

    public void connect(int port, int counter, boolean eventArrive, boolean useTLS, int expectedEventCount)
            throws InterruptedException {

        results = new ArrayList<>(expectedEventCount);
        count = counter;
        eventArrived = eventArrive;
        if (useTLS) {
            CustomCertificateTlsSocketFactory tlsFac = new CustomCertificateTlsSocketFactory();
            String tlsKeystoreFilepath = "src/test/resources/security/wso2carbon.jks";
            tlsFac.setKeystoreFilename(tlsKeystoreFilepath);
            String tlsKeystorePassphrase = "wso2carbon";
            tlsFac.setKeystorePassphrase(tlsKeystorePassphrase);
            context.setSocketFactory(new HapiSocketTlsFactoryWrapper(tlsFac));
        }
        hl7Service = context.newServer(port, useTLS);
        hl7Service.startAndWait();
        log.info("SERVER WORKING");
        hl7Service.registerApplication(new RegistrationEventRout(), new TestApp());
    }

    public void connectWithTlsPath(int port, int counter, boolean eventArrive, boolean useTLS,
                                   int expectedEventCount, String tlsPath, String pass) throws InterruptedException {

        results = new ArrayList<>(expectedEventCount);
        count = counter;
        eventArrived = eventArrive;
        if (useTLS) {
            CustomCertificateTlsSocketFactory tlsFac = new CustomCertificateTlsSocketFactory();
            tlsFac.setKeystoreFilename(tlsPath);
            tlsFac.setKeystorePassphrase(pass);
            context.setSocketFactory(new HapiSocketTlsFactoryWrapper(tlsFac));
        }
        hl7Service = context.newServer(port, useTLS);
        hl7Service.startAndWait();
        log.info("SERVER WORKING");
        hl7Service.registerApplication(new RegistrationEventRout(), new TestApp());
    }

    public int getCount() {

        return count;
    }

    public boolean getEventArrived() {

        return eventArrived;
    }

    public boolean assertMessageContent(String content) {

        for (String controlID : results) {
            if (controlID.equals(content)) {
                return true;
            }
        }
        return false;
    }

    class TestApp implements ReceivingApplication {

        @Override
        public Message processMessage(Message message, Map<String, Object> theMetadata) throws HL7Exception {

            try {
                count++;
                eventArrived = true;
                results.add(testUtil.getControlID(message));
                return message.generateACK();
            } catch (IOException e) {
                throw new HL7Exception(e);
            }
        }

        @Override
        public boolean canProcess(Message theMessage) {

            return true;
        }
    }

    class RegistrationEventRout implements ApplicationRouter.AppRoutingData {

        @Override
        public String getVersion() {

            return "*";
        }

        @Override
        public String getTriggerEvent() {

            return "*";
        }

        @Override
        public String getProcessingId() {

            return "*";
        }

        @Override
        public String getMessageType() {

            return "*";
        }
    }

}
