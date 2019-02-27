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

import ca.uhn.hl7v2.hoh.util.IOUtils;
import ca.uhn.hl7v2.hoh.util.KeystoreUtils;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Locale;

/**
 * This class contains the utility functions required to the Hl7 extension.
 */
public class Hl7Utils {

    /**
     * Handles Validation Exceptions for hl7 Encoding types.
     *
     * @param hl7Encoding      - Encoding type of hl7 receiving message
     * @param hl7AckEncoding   - Encoding type of hl7 acknowledgement message
     * @param siddhiAppName    - Defined siddhi app name
     * @param streamID         - defined stream id
     */
    public static void validateEncodingType(String hl7Encoding, String hl7AckEncoding, String siddhiAppName,
                                            String streamID) {

        if (!(hl7Encoding.toUpperCase(Locale.ENGLISH).equals("ER7") ||
                hl7Encoding.toUpperCase(Locale.ENGLISH).equals("XML"))) {
            throw new SiddhiAppValidationException("Invalid hl7.encoding type defined in " + siddhiAppName + ":" +
                    streamID + ". hl7.encoding type should be er7 or xml.");

        }
        if (!(hl7AckEncoding.toUpperCase(Locale.ENGLISH).equals("ER7") ||
                hl7AckEncoding.toUpperCase(Locale.ENGLISH).equals("XML"))) {
            throw new SiddhiAppValidationException("Invalid hl7.ack.encoding type defined in " + siddhiAppName + ":" +
                    streamID + ". hl7.ack.encoding type should be er7 or xml.");

        }
    }

    /**
     * Handles Validation Exceptions for Enabling Tls
     *
     * @param tlsEnabled            - Check whether tls Enabled or not
     * @param tlsKeystoreFilepath   - Filepath of the keystore
     * @param tlsKeystorePassphrase - Passphrase of the keystore
     * @param tlsKeystoreType       - Type of the keystore
     * @param siddhiAppName         - Defined siddhi app name
     * @param streamID              - defined stream id
     */
    public static void doTlsValidation(boolean tlsEnabled, String tlsKeystoreFilepath, String tlsKeystorePassphrase,
                                String tlsKeystoreType, String siddhiAppName, String streamID) {

        if (tlsEnabled) {
            try {
                KeyStore keyStore = KeystoreUtils.loadKeystore(tlsKeystoreFilepath, tlsKeystorePassphrase);
                KeyStore.getInstance(tlsKeystoreType);
                KeystoreUtils.validateKeystoreForTlsSending(keyStore);
            } catch (FileNotFoundException e) {
                throw new SiddhiAppCreationException("Failed to found the keystore file." +
                        " Please check the tls.keystore.filepath = " + tlsKeystoreFilepath + " defined in " +
                        siddhiAppName + ":" + streamID + ". ", e);
            } catch (IOException e) {
                throw new SiddhiAppCreationException("Failed to load keystore. Please check the " +
                        "tls.keystore.filepath = " + tlsKeystoreFilepath + " defined in " + siddhiAppName + ":" +
                        streamID + ". ", e);
            } catch (CertificateException | NoSuchAlgorithmException e) {
                throw new SiddhiAppCreationException("Failed to load keystore. please check the keystore defined in" +
                        siddhiAppName + ":" + streamID + ". ", e);
            } catch (KeyStoreException e) {
                throw new SiddhiAppCreationException("Failed to load keystore. Please check " +
                        "the tls.keystore.type = " + tlsKeystoreType + "  defined in " + siddhiAppName + ":" +
                        streamID + ". ", e);
            }
        }
    }


    /**
     * Used to parse the inputStream to String type
     *
     * @param in - fileInputStream
     * @return String type of inputStream
     */
    public static String streamToString(InputStream in) throws IOException {

        ByteBuffer buffer = ByteBuffer.wrap(IOUtils.readInputStreamIntoByteArray(in));
        return new String(buffer.array(), 0, buffer.limit(), StandardCharsets.UTF_8);
    }
}
