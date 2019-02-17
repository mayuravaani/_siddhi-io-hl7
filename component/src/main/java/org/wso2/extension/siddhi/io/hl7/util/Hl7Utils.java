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
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * This class contains the utility functions required to the Hl7 extension.
 */
public class Hl7Utils {

    /**
     * Handles Validation Exceptions for uri.
     *
     * @param uri              - uri that is used to connect the server
     * @param siddhiStreamName - defined stream id
     */
    public static int getValuesFromURI(String uri, String siddhiStreamName) {

        String[] separator = uri.split(":");
        if (separator.length == 2) {
            try {
                Integer.parseInt(separator[1]);
                return 2;
            } catch (NumberFormatException e) {
                throw new SiddhiAppValidationException("Invalid uri format defined in " + siddhiStreamName +
                        ". Please check the port number.");
            }
        } else if (separator.length == 3) {
            if (separator[0].toUpperCase(Locale.ENGLISH).equals("HL7")) {
                try {
                    Integer.parseInt(separator[2]);
                    return 3;
                } catch (NumberFormatException e) {
                    throw new SiddhiAppValidationException("Invalid uri format defined in " + siddhiStreamName +
                            ". Please check the port number.");
                }
            } else {
                throw new SiddhiAppValidationException("Invalid uri format defined in " + siddhiStreamName +
                        ". Expected uri format is hl7://{host}:{port} or {host}:{port}.");
            }

        } else {
            throw new SiddhiAppValidationException("Invalid uri format defined in " + siddhiStreamName +
                    ". Expected uri format is hl7://{host}:{port} or {host}:{port}.");
        }
    }

    /**
     * Handles Validation Exceptions for hl7 Encoding types.
     *
     * @param hl7Encoding      - Encoding type of hl7 receiving message
     * @param hl7AckEncoding   - Encoding type of hl7 acknowledgement message
     * @param siddhiStreamName - defined stream id
     */
    public static void validateEncodingType(String hl7Encoding, String hl7AckEncoding, String siddhiStreamName) {

        if (!(hl7Encoding.toUpperCase(Locale.ENGLISH).equals("ER7") ||
                hl7Encoding.toUpperCase(Locale.ENGLISH).equals("XML"))) {
            throw new SiddhiAppValidationException("Invalid hl7.encoding type defined in " + siddhiStreamName +
                    ". hl7.encoding type should be er7 or xml.");

        }
        if (!(hl7AckEncoding.toUpperCase(Locale.ENGLISH).equals("ER7") ||
                hl7AckEncoding.toUpperCase(Locale.ENGLISH).equals("XML"))) {
            throw new SiddhiAppValidationException("Invalid hl7.ack.encoding type defined in " + siddhiStreamName +
                    ". hl7.ack.encoding type should be er7 or xml.");

        }
    }

    /**
     * Used to parse the inputStream to String type
     *
     * @param in fileInputStream
     * @return String type of inputStream
     */
    public static String streamToString(InputStream in) throws IOException {

        ByteBuffer buffer = ByteBuffer.wrap(IOUtils.readInputStreamIntoByteArray(in));
        return new String(buffer.array(), 0, buffer.limit(), StandardCharsets.UTF_8);
    }
}
