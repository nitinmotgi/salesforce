/*
* Copyright 2018 Google Inc. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/


package com.google;

import com.google.force.ForceConnectorHelper;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class TestAuth {

    public static void main(String[] argv) {


        ForceConnectorHelper fconnect = new ForceConnectorHelper(
                2000,
                "3MVG9vrJTfRxlfl4rYxjevCCVFhbmeskvIvEy5oF_8FaOgEvd5fKFUG7wxh1L3hS5loZkqpR5XbS2_9RCtkyO",
               "1254813275695943430",
                "parviz_deyhim@yahoo.com",
                "w9igPrq2kzNDG9tBG6dzlbycmfMEqJYNXYTnIwK",
                "LeadUpdates",
                "/cometd/39.0");

        fconnect.start();
        //fconnect.getMessages();
    }
}
