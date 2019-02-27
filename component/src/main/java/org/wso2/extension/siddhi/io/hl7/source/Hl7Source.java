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

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.app.HL7Service;
import ca.uhn.hl7v2.conf.ProfileException;
import ca.uhn.hl7v2.conf.parser.ProfileParser;
import ca.uhn.hl7v2.conf.spec.RuntimeProfile;
import ca.uhn.hl7v2.hoh.sockets.CustomCertificateTlsSocketFactory;
import ca.uhn.hl7v2.hoh.util.HapiSocketTlsFactoryWrapper;
import ca.uhn.hl7v2.llp.MinLowerLayerProtocol;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.hl7.util.Hl7Constants;
import org.wso2.extension.siddhi.io.hl7.util.Hl7Utils;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Hl7 Source Implementation
 */

@Extension(
        name = "hl7",
        namespace = "source",
        description = "The hl7 source consumes the hl7 messages using MLLP protocol. ",
        parameters = {
                @Parameter(name = "port",
                        description = "This is the unique logical address used to establish the connection for " +
                                "the process. ",
                        type = {DataType.INT}),

                @Parameter(name = "hl7.encoding",
                        description = "Encoding method of received hl7. This can be er7 or xml. User should define " +
                                "hl7 encoding type according to their mapping. \n" +
                                "e.g., \n" +
                                "If text mapping is used, then the hl7 encoding type should be er7. \n" +
                                "If xml mapping is used, then the hl7 encoding type should be xml. ",
                        type = {DataType.STRING}),

                @Parameter(name = "hl7.ack.encoding",
                        description = "Encoding method of hl7 to log the acknowledgment message. This parameter " +
                                "can be specified as xml if required. Otherwise, system uses er7 format as default. ",
                        optional = true, defaultValue = "ER7",
                        type = {DataType.STRING}),

                @Parameter(name = "charset",
                        description = "Character encoding method. Charset can be specified if required. Otherwise, " +
                                "system uses UTF-8 as default charset. ",
                        optional = true, defaultValue = "UTF-8",
                        type = {DataType.STRING}),

                @Parameter(name = "tls.enabled",
                        description = "This parameter specifies whether an encrypted communication should " +
                                "be established or not. When this parameter is set to `true`, the " +
                                "`tls.keystore.path` and `tls.keystore.passphrase` parameters are initialized. ",
                        optional = true, defaultValue = "false",
                        type = {DataType.BOOL}),

                @Parameter(name = "tls.keystore.filepath",
                        description = "The file path to the location of the keystore of the client that sends" +
                                "the HL7 events via the `MLLP` protocol. A custom keystore can be" +
                                "specified if required. If a custom keystore is not specified, then the system" +
                                "uses the default `wso2carbon` keystore in the `${carbon.home}/resources/security` " +
                                "directory. ",
                        optional = true, defaultValue = "${carbon.home}/resources/security/wso2carbon.jks",
                        type = {DataType.STRING}),

                @Parameter(name = "tls.keystore.type",
                        description = "The type for the keystore. A custom keystore type can be specified " +
                                "if required. If no custom keystore type is specified, then the system uses " +
                                "`JKS` as the default keystore type.",
                        optional = true, defaultValue = "JKS",
                        type = {DataType.STRING}),

                @Parameter(name = "tls.keystore.passphrase",
                        description = "The passphrase for the keystore. A custom passphrase can be specified " +
                                "if required. If no custom passphrase is specified, then the system uses " +
                                "`wso2carbon` as the default passphrase.",
                        optional = true, defaultValue = "wso2carbon",
                        type = {DataType.STRING}),

                @Parameter(name = "hl7.conformance.profile.used",
                        description = "This parameter specifies whether a `conformance profile` is used to validate " +
                                "the incoming message or not. When the parameter is set to `true`, the " +
                                "hl7.conformance.profile.file.name should be initialized by user. If the conformance " +
                                "profile is used then It will send the error details along with the acknowledgment. ",
                        optional = true, defaultValue = "false",
                        type = {DataType.BOOL}),

                @Parameter(name = "hl7.conformance.profile.file.path",
                        description = "Path conformance profile file that is used to validate the incoming " +
                                "message. User should give the file path, if conformance profile is used to validate " +
                                "the message. ",
                        optional = true, defaultValue = "Empty",
                        type = {DataType.STRING})

        },
        examples = {
                @Example(
                        syntax = "@App:name('Hl7TestAppForTextMapping') \n" +
                                "@source(type = 'hl7', \n" +
                                "port = '1080', \n" +
                                "hl7.encoding = 'er7', \n" +
                                "@map(type = 'text'))\n" +
                                "define stream hl7stream(payload string); \n"
                        ,
                        description = "This receives the HL7 messages and sends the acknowledgement message to the " +
                                "client using the MLLP protocol and text mapping. \n "
                ),
                @Example(
                        syntax = "@App:name('Hl7TestAppForXMLMapping') \n" +
                                "@source(type = 'hl7', \n" +
                                "port = '1080', \n" +
                                "hl7.encoding = 'xml', \n" +
                                "@map(type = 'xml', namespaces = 'ns=urn:hl7-org:v2xml', @attributes(" +
                                "MSH10 = \"ns:MSH/ns:MSH.10\", MSH3HD1 = \"ns:MSH/ns:MSH.3/ns:HD.1\")))\n" +
                                "define stream hl7stream (MSH10 string, MSH3HD1 string); \n"
                        ,
                        description = "This receives the HL7 messages nd send the acknowledgement message to the " +
                                "client using the MLLP protocol and custom xml mapping. \n "
                )
        }
)
public class Hl7Source extends Source {

    private static final Logger log = Logger.getLogger(Hl7Source.class);
    private SourceEventListener sourceEventListener;
    private int port;
    private boolean tlsEnabled;
    private String hl7Encoding;
    private String hl7AckEncoding;
    private String charset;
    private HL7Service hl7Service;
    private String tlsKeystoreFilepath;
    private String tlsKeystorePassphrase;
    private Hl7ReceivingApp hl7ReceivingApp;
    private boolean conformanceProfileUsed;
    private RuntimeProfile conformanceProfile;
    private String tlsKeystoreType;
    private String streamID;
    private String siddhiAppName;
    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                     String[] requestedTransportPropertyNames, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {

        this.sourceEventListener = sourceEventListener;
        this.streamID = sourceEventListener.getStreamDefinition().getId();
        this.siddhiAppName = siddhiAppContext.getName();
        this.port = Integer.parseInt(optionHolder.validateAndGetStaticValue(Hl7Constants.HL7_PORT_NO));
        this.hl7Encoding = optionHolder.validateAndGetStaticValue(Hl7Constants.HL7_ENCODING);
        this.hl7AckEncoding = optionHolder.validateAndGetStaticValue(Hl7Constants.ACK_HL7_ENCODING,
                Hl7Constants.DEFAULT_ACK_HL7_ENCODING);
        this.charset = optionHolder.validateAndGetStaticValue(Hl7Constants.CHARSET_NAME,
                Hl7Constants.DEFAULT_HL7_CHARSET);
        this.tlsEnabled = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(Hl7Constants.TLS_ENABLE,
                Hl7Constants.DEFAULT_TLS_ENABLE));
        this.tlsKeystoreFilepath = optionHolder.validateAndGetStaticValue(Hl7Constants.TLS_KEYSTORE_FILEPATH,
                Hl7Constants.DEFAULT_TLS_KEYSTORE_FILEPATH);
        this.tlsKeystorePassphrase = optionHolder.validateAndGetStaticValue(Hl7Constants.TLS_KEYSTORE_PASSPHRASE,
                Hl7Constants.DEFAULT_TLS_KEYSTORE_PASSPHRASE);
        this.conformanceProfileUsed = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(
                Hl7Constants.HL7_CONFORMANCE_PROFILE_USED,
                Hl7Constants.DEFAULT_CONFORMANCE_PROFILE_USED));
        String profileFileName = optionHolder.validateAndGetStaticValue(Hl7Constants.HL7_CONFORMANCE_PROFILE_FILE,
                Hl7Constants.DEFAULT_CONFORMANCE_PROFILE_FILE);
        Hl7Utils.validateEncodingType(hl7Encoding, hl7AckEncoding, siddhiAppName, streamID);
        this.hl7ReceivingApp = new Hl7ReceivingApp();
        this.tlsKeystoreType = optionHolder.validateAndGetStaticValue(Hl7Constants.TLS_KEYSTORE_TYPE,
                Hl7Constants.DEFAULT_TLS_KEYSTORE_TYPE);
        if (conformanceProfileUsed) {
            conformanceProfile = getConformanceProfile(profileFileName);
        }
        Hl7Utils.doTlsValidation(tlsEnabled, tlsKeystoreFilepath, tlsKeystorePassphrase, tlsKeystoreType,
                siddhiAppName, streamID);
    }

    @Override
    public Class[] getOutputEventClasses() {

        return new Class[]{String.class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {

        HapiContext hapiContext = new DefaultHapiContext();
        MinLowerLayerProtocol mllp = new MinLowerLayerProtocol();
        mllp.setCharset(charset);
        hapiContext.setLowerLayerProtocol(mllp);
        if (tlsEnabled) {
            CustomCertificateTlsSocketFactory tlsFac = new CustomCertificateTlsSocketFactory(tlsKeystoreType,
                    tlsKeystoreFilepath, tlsKeystorePassphrase);
            hapiContext.setSocketFactory(new HapiSocketTlsFactoryWrapper(tlsFac));
        }
        hl7Service = hapiContext.newServer(port, tlsEnabled);
        try {
            hl7Service.startAndWait();
        } catch (InterruptedException e) {
            throw new ConnectionUnavailableException("Error occurred while starting the server on port: " + port
                    + ", ", e);
        }
        hl7Service.registerApplication(new RegistrationEventRouting(), new Hl7ReceivingApp(sourceEventListener,
                siddhiAppName, streamID, hl7Encoding, hl7AckEncoding, hapiContext, conformanceProfileUsed,
                conformanceProfile));
        hl7Service.setExceptionHandler(new Hl7ExceptionHandler());
    }

    @Override
    public void disconnect() {

        if (hl7Service != null) {
            hl7Service.stop();
        }

    }

    @Override
    public void destroy() {

    }

    @Override
    public void pause() {

        hl7ReceivingApp.pause();
    }

    @Override
    public void resume() {

        hl7ReceivingApp.resume();
    }

    @Override
    public Map<String, Object> currentState() {

        return null;
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        //No state to restore
    }

    private RuntimeProfile getConformanceProfile(String profileFileName) {

        if (!profileFileName.equals("")) {
            InputStream in = null;
            try {
                ProfileParser profileParser = new ProfileParser(false);
                in = new FileInputStream(profileFileName);
                String file = Hl7Utils.streamToString(in);
                return profileParser.parse(file);
            } catch (ProfileException e) {
                throw new SiddhiAppCreationException("The conformance Profile file given in" + siddhiAppName + ":" +
                        streamID + " is not supported. Hence, dropping the profile validation. ", e);
            } catch (IOException e) {
                throw new SiddhiAppCreationException("Failed to load the given conformance profile file given in " +
                        siddhiAppName + ":" + streamID + " Hence, dropping the profile validation. ", e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        log.error(e);
                    }
                }
            }
        } else {
            throw new SiddhiAppCreationException("Empty field has been found in hl7.conformance.profile.file.path " +
                    "defined in " + siddhiAppName + ":" + streamID + ", Please prefer your file name. Hence, " +
                    "dropping the validation. ");
        }
    }
}
