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

package com.google.cdap;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import javax.xml.namespace.QName;

/**
 * This class <code>SessionRenewer</code> handles renewing of the session when an expired session is detected.
 */
public class SessionRenewer implements com.sforce.ws.SessionRenewer {
  @Override
  public SessionRenewalHeader renewSession(ConnectorConfig config) throws ConnectionException {
    PartnerConnection connection = Connector.newConnection(config);
    SessionRenewalHeader header = new SessionRenewalHeader();
    header.name = new QName("urn:enterprise.soap.sforce.com", "SessionHeader");
    header.headerElement = connection.getSessionHeader();
    return header;
  }
}
