/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.hydrator.common.ReferencePluginConfig;

import javax.annotation.Nullable;

public class BasicSalesforceConfig extends ReferencePluginConfig {

  @Description("Your Salesforce connected app's client ID")
  @Macro
  private String clientId;

  @Description("Your Salesforce connected app's client secret key")
  @Macro
  private String clientSecret;

  @Description("Your Salesforce username")
  @Macro
  private String username;

  @Description("Your Salesforce password")
  @Macro
  private String password;

  @Description("The Salesforce object to read from")
  @Macro
  private String object;

  @Description("The Force API version to use. Defaults to 45.")
  @Nullable
  @Macro
  private final String apiVersion;

  @Description("The Login URL to use to authenticate to Salesforce. Defaults to " +
    "https://login.salesforce.com/services/oauth2/token")
  @Nullable
  @Macro
  private final String loginUrl;

  BasicSalesforceConfig() {
    super("input");
    this.apiVersion = "45.0";
    this.loginUrl = "https://login.salesforce.com/services/oauth2/token";
  }

  public BasicSalesforceConfig(String referenceName, String clientId, String clientSecret, String username,
                               String password, String object, @Nullable String apiVersion, @Nullable String loginUrl) {
    super(referenceName);
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.username = username;
    this.password = password;
    this.object = object;
    this.apiVersion = apiVersion;
    this.loginUrl = loginUrl;
  }

  public String getClientId() {
    return clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getObject() {
    return object;
  }

  @Nullable
  public String getApiVersion() {
    return apiVersion;
  }

  @Nullable
  public String getLoginUrl() {
    return loginUrl;
  }
}
