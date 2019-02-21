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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;

@VisibleForTesting
public final class AuthResponse {
  @SerializedName("access_token")
  private final String accessToken;
  @SerializedName("instance_url")
  private final String instanceUrl;
  private final String id;
  @SerializedName("token_type")
  private final String tokenType;
  @SerializedName("issued_at")
  private final String issuedAt;
  private final String signature;

  private AuthResponse(String accessToken, String instanceUrl, String id, String tokenType,
                       String issuedAt, String signature) {
    this.accessToken = accessToken;
    this.instanceUrl = instanceUrl;
    this.id = id;
    this.tokenType = tokenType;
    this.issuedAt = issuedAt;
    this.signature = signature;
  }

  public String getAccessToken() {
    return accessToken;
  }

  public String getInstanceUrl() {
    return instanceUrl;
  }

  public String getId() {
    return id;
  }

  public String getTokenType() {
    return tokenType;
  }

  public String getIssuedAt() {
    return issuedAt;
  }

  public String getSignature() {
    return signature;
  }
}
