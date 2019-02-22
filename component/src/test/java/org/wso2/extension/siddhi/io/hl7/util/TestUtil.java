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

import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v23.message.ADT_A01;
import ca.uhn.hl7v2.model.v23.message.ADT_A04;
import ca.uhn.hl7v2.model.v23.message.BAR_P01;
import ca.uhn.hl7v2.model.v23.message.BAR_P02;
import ca.uhn.hl7v2.model.v23.message.ORM_O01;
import ca.uhn.hl7v2.model.v23.message.ORU_R01;
import ca.uhn.hl7v2.model.v23.segment.MSH;

/**
 * This class used to get the control id of the different message types of hl7.
 */
public class TestUtil {

    private MSH msgSegment;

    public TestUtil() {

        msgSegment = null;
    }

    public String getControlID(Message message) {

        if (message instanceof ADT_A01) {
            ADT_A01 adtA01 = (ADT_A01) message;
            msgSegment = adtA01.getMSH();
        } else if (message instanceof ADT_A04) {
            ADT_A04 adtA04 = (ADT_A04) message;
            msgSegment = adtA04.getMSH();
        } else if (message instanceof ORU_R01) {
            ORU_R01 oruR01 = (ORU_R01) message;
            msgSegment = oruR01.getMSH();
        } else if (message instanceof ORM_O01) {
            ORM_O01 ormO01 = (ORM_O01) message;
            msgSegment = ormO01.getMSH();
        } else if (message instanceof BAR_P01) {
            BAR_P01 barP01 = (BAR_P01) message;
            msgSegment = barP01.getMSH();
        } else if (message instanceof BAR_P02) {
            BAR_P02 barP02 = (BAR_P02) message;
            msgSegment = barP02.getMSH();
        }
        return msgSegment.getMessageControlID().toString();
    }

}
