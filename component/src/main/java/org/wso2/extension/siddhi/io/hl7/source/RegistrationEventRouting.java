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

import ca.uhn.hl7v2.protocol.ApplicationRouter;

/**
 * RegistrationEventRouting is used to route messages to the appropriate application.
 * This class handles all Message Types and Trigger Events.
 */
public class RegistrationEventRouting implements ApplicationRouter.AppRoutingData {

    @Override
    public String getMessageType() {

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
    public String getVersion() {

        return "*";
    }
}
