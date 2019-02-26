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
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.conf.ProfileException;
import ca.uhn.hl7v2.conf.check.DefaultValidator;
import ca.uhn.hl7v2.conf.spec.RuntimeProfile;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.Parser;
import ca.uhn.hl7v2.protocol.ReceivingApplication;
import ca.uhn.hl7v2.protocol.ReceivingApplicationException;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.hl7.source.exception.Hl7SourceRuntimeException;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * HL7ReceivingApp is a consumer of a message and it process the message returns the acknowledgement.
 */

public class Hl7ReceivingApp implements ReceivingApplication {

    private static final Logger log = Logger.getLogger(Hl7ReceivingApp.class);
    private SourceEventListener sourceEventListener;
    private String hl7EncodeType;
    private String hl7AckType;
    private boolean paused;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    private HapiContext hapiContext;
    private boolean conformanceUsed;
    private RuntimeProfile conformanceProfile;
    private String siddhiAppName;
    private String streamID;

    public Hl7ReceivingApp() {

    }

    /**
     * Handles Processing of the Receiving Messages.
     *
     * @param hapiContext         - context that is used to configure the Hapi core services
     * @param hl7EncodeType       - Encoding type of hl7 receiving message
     * @param hl7AckType          - Encoding type of hl7 acknowledgement message
     * @param conformanceUsed     - Conformance profile is used or not
     * @param conformanceProfile  - Conformance profile file name
     * @param sourceEventListener - listens events
     * @param streamID            - the stream name of the siddhiApp
     * @param siddhiAppName       - the name of the siddhiApp
     */
    public Hl7ReceivingApp(SourceEventListener sourceEventListener, String siddhiAppName, String streamID,
                           String hl7EncodeType, String hl7AckType, HapiContext hapiContext, boolean conformanceUsed,
                           RuntimeProfile conformanceProfile) {

        this.sourceEventListener = sourceEventListener;
        this.siddhiAppName = siddhiAppName;
        this.streamID = streamID;
        this.hl7EncodeType = hl7EncodeType;
        this.hl7AckType = hl7AckType;
        this.hapiContext = hapiContext;
        this.conformanceUsed = conformanceUsed;
        this.conformanceProfile = conformanceProfile;

    }

    @Override
    public Message processMessage(Message message, Map<String, Object> metaData)
            throws HL7Exception, ReceivingApplicationException {

        Parser pipeParser = hapiContext.getPipeParser();
        Parser xmlParser = hapiContext.getXMLParser();
        if (paused) { //spurious wakeup condition is deliberately traded off for performance
            lock.lock();
            try {
                while (paused) {
                    condition.await();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }
        if (hl7EncodeType.toUpperCase(Locale.ENGLISH).equals("ER7")) {
            String er7Msg = pipeParser.encode(message);
            sourceEventListener.onEvent("payload: " + "'" + er7Msg + "'", null);
        } else {
            String xmlMsg = xmlParser.encode(message);
            sourceEventListener.onEvent(xmlMsg, null);
        }
        Message ackMsg;
        try {
            ackMsg = message.generateACK();
        } catch (IOException e) {
            throw new ReceivingApplicationException("Error: ", e);
        }
        if (conformanceUsed) {
            HL7Exception[] problems;
            try {
                problems = new DefaultValidator().validate(message,
                        conformanceProfile.getMessage());
            } catch (ProfileException e) {
                throw new HL7Exception(e);
            }
            if (problems.length > 0) {
                throw new Hl7SourceRuntimeException("The following validation errors were found during " +
                        "message validation: \n" + Arrays.toString(problems) + "\n");
            }
        }
        if (hl7AckType.toUpperCase(Locale.ENGLISH).equals("ER7")) {
            String er7AckMsg = pipeParser.encode(ackMsg);
            log.info("Sent Acknowledgement for stream " + siddhiAppName + ":" + streamID + ": \n" +
                    er7AckMsg.replaceAll("\r", "\n"));
        } else {
            String xmlAckMsg = xmlParser.encode(ackMsg);
            log.info("Sent Acknowledgement for stream " + siddhiAppName + ":" + streamID + ": \n" + xmlAckMsg);
        }
        return ackMsg;
    }

    @Override
    public boolean canProcess(Message message) {

        return true;
    }

    public void pause() {

        paused = true;
    }

    public void resume() {

        paused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
