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

import com.sforce.ws.ConnectorConfig;

/**
 * This class <code>PartnerConfig</code> defines the basic configuration required to configure
 * invoking partner API.
 */
public final class PartnerConfig {
  private final String username;
  private final String password;
  private final String endPoint;
  private final String version;
  private final boolean compression;

  public PartnerConfig(String username, String password, String endPoint, String version, boolean compression) {
    this.username = username;
    this.password = password;
    this.endPoint = endPoint;
    this.version = version;
    this.compression = compression;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getEndPoint() {
    return endPoint;
  }

  public String getVersion() {
    return version;
  }

  public boolean getCompression() {
    return compression;
  }

  public ConnectorConfig getConnectorConfig() {
    ConnectorConfig config = new ConnectorConfig();
    config.setUsername(username);
    config.setPassword(password);
    config.setAuthEndpoint("https://" + endPoint + "/services/Soap/u/" + version);
    config.setCompression(compression);
    config.setSessionRenewer(new SessionRenewer());
    return config;
  }
}
