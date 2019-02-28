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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;
import com.sforce.ws.ConnectorConfig;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public final class SalesforceBulkAPIs {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceBulkAPIs.class);
  private static final Gson GSON = new Gson();

  private SalesforceBulkAPIs() {
  }

  /**
   * Create the BulkConnection used to call Bulk API operations.
   */
  public static BulkConnection getBulkConnection(String loginUrl, String clientId, String clientSecret,
                                                 String username, String password, String apiVersion) throws Exception {
    AuthResponse authResponse = oauthLogin(loginUrl, clientId, clientSecret, username, password);
    ConnectorConfig connectorConfig = new ConnectorConfig();
    connectorConfig.setSessionId(authResponse.getAccessToken());
    // https://instance_name—api.salesforce.com/services/async/APIversion/job/jobid/batch
    String restEndpoint = String.format("%s/services/async/%s", authResponse.getInstanceUrl(), apiVersion);
    connectorConfig.setRestEndpoint(restEndpoint);
    // This should only be false when doing debugging.
    connectorConfig.setCompression(true);
    // Set this to true to see HTTP requests and responses on stdout
    connectorConfig.setTraceMessage(false);
    return new BulkConnection(connectorConfig);
  }

  private static AuthResponse oauthLogin(String loginUrl, String clientId, String clientSecret,
                                         String username, String password) throws Exception {
    SslContextFactory sslContextFactory = new SslContextFactory();
    HttpClient httpClient = new HttpClient(sslContextFactory);
    try {
      httpClient.start();
      String response = httpClient.POST(loginUrl).param("grant_type", "password")
        .param("client_id", clientId)
        .param("client_secret", clientSecret)
        .param("username", username)
        .param("password", password).send().getContentAsString();
      return GSON.fromJson(response, AuthResponse.class);
    } finally {
      httpClient.stop();
    }
  }

  /**
   * Create a new job using the Bulk API.
   *
   * @return The JobInfo for the new job.
   * @throws AsyncApiException if there is an issue creating the job
   */
  public static JobInfo createJob(String sObject, BulkConnection bulkConnection) throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setObject(sObject);
    job.setOperation(OperationEnum.query);
    job.setConcurrencyMode(ConcurrencyMode.Parallel);
    job.setContentType(ContentType.CSV);
    job = bulkConnection.createJob(job);
    Preconditions.checkState(job.getId() != null, "Couldn't get job ID. Perhaps there was a problem in creating the " +
      "batch job");
    return bulkConnection.getJobStatus(job.getId());
  }

  public static List<String> runBulkQuery(String query, BulkConnection bulkConnection,
                                          JobInfo job) throws IOException, AsyncApiException {
    String[] queryResults = null;
    BatchInfo info;
    try (ByteArrayInputStream bout = new ByteArrayInputStream(query.getBytes())) {
      info = bulkConnection.createBatchFromStream(job, bout);
      for (int i = 0; i < 10000; i++) {
        info = bulkConnection.getBatchInfo(job.getId(), info.getId());
        if (BatchStateEnum.Completed == info.getState()) {
          QueryResultList list = bulkConnection.getQueryResultList(job.getId(), info.getId());
          queryResults = list.getResult();
          break;
        } else if (BatchStateEnum.Failed == info.getState()) {
          LOG.error("Failed " + info);
          break;
        } else {
          LOG.debug("Waiting " + info);
        }
      }
    }

    List<String> results = new ArrayList<>();
    if (queryResults != null) {
      for (String resultId : queryResults) {
        InputStream queryResultStream = bulkConnection.getQueryResultStream(job.getId(), info.getId(), resultId);
        results.add(CharStreams.toString(new InputStreamReader(queryResultStream, Charsets.UTF_8)));
      }
    }
    return results;
  }
}
