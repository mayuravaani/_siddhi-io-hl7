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
package org.wso2.extension.siddhi.io.hl7.util;

/**
 * This class represents Hl7 specific Constants.
 */
public class Hl7Constants {

    public static final String HL7_PORT_NO = "port";
    public static final String HL7_URI = "uri";
    public static final String CHARSET_NAME = "charset";
    public static final String TLS_ENABLE = "tls.enabled";
    public static final String HL7_ENCODING = "hl7.encoding";
    public static final String ACK_HL7_ENCODING = "hl7.ack.encoding";
    public static final String HL7_TIMEOUT = "hl7.timeout";
    public static final String DEFAULT_HL7_CHARSET = "UTF-8";
    public static final String DEFAULT_TLS_ENABLE = "false";
    public static final String DEFAULT_HL7_TIMEOUT = "10000";
    public static final String DEFAULT_ACK_HL7_ENCODING = "ER7";
    public static final String TLS_KEYSTORE_FILEPATH = "tls.keystore.filepath";
    public static final String TLS_KEYSTORE_PASSPHRASE = "tls.keystore.passphrase";
    public static final String TLS_KEYSTORE_TYPE = "tls.keystore.type";
    public static final String DEFAULT_TLS_KEYSTORE_TYPE = "jks";
    public static final String DEFAULT_TLS_KEYSTORE_FILEPATH =
            System.getProperty("carbon.home") + "/resources/security/wso2carbon.jks";
    public static final String DEFAULT_TLS_KEYSTORE_PASSPHRASE = "wso2carbon";
    public static final String HL7_CONFORMANCE_PROFILE_USED = "hl7.conformance.profile.used";
    public static final String DEFAULT_CONFORMANCE_PROFILE_USED = "false";
    public static final String HL7_CONFORMANCE_PROFILE_FILE = "hl7.conformance.profile.file.path";
    public static final String DEFAULT_CONFORMANCE_PROFILE_FILE = "";
}
