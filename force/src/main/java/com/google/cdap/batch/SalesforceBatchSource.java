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

package com.google.cdap.batch;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;
import com.sforce.ws.ConnectorConfig;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;


@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SalesforceBatchSource.NAME)
@Description("PLugin to read data from Salesforce in batches.")
public class SalesforceBatchSource extends BatchSource {
  static final String NAME = "Salesforce";
  private static final String AUTH_URL = "https://login.salesforce.com/services/oauth2/token";
  private static final Gson GSON = new Gson();

  private final Config config;

  @VisibleForTesting
  SalesforceBatchSource(Config config) {
    this.config = config;
  }

  @VisibleForTesting
  static final class Config extends ReferencePluginConfig {

    @Description("Your Salesforce connected app's client ID")
    @Macro
    private final String clientId;

    @Description("Your Salesforce connected app's client secret key")
    @Macro
    private final String clientSecret;

    @Description("Your Salesforce username")
    @Macro
    private final String username;

    @Description("Your Salesforce password")
    @Macro
    private final String password;

    @Description("The Salesforce instance name")
    @Macro
    private final String instance;

    @Description("The Salesforce object to read from")
    @Macro
    private final String object;

    @Description("The SOQL query to retrieve results for")
    @Macro
    private final String query;

    @Description("The Force API version to use. Defaults to 45.")
    @Nullable
    @Macro
    private final String apiVersion;

    Config(String referenceName, String clientId, String clientSecret,
           String username, String password, String instance, String object, String query) {
      this(referenceName, clientId, clientSecret, username, password, instance, object, query, "45");
    }

    Config(String referenceName, String clientId, String clientSecret,
           String username, String password, String instance, String object, String query, String apiVersion) {
      super(referenceName);
      this.clientId = clientId;
      this.clientSecret = clientSecret;
      this.username = username;
      this.password = password;
      this.instance = instance;
      this.object = object;
      this.query = query;
      this.apiVersion = apiVersion;
    }
  }

  @Override
  public void initialize(Object o) throws Exception {

  }

  @Override
  public void prepareRun(BatchContext batchContext) throws Exception {

  }

  @Override
  public void onRunFinish(boolean b, Object o) {
    //closeJob(, );
  }

  @Override
  public void transform(Object o, Emitter emitter) throws Exception {

  }

  @VisibleForTesting
  List<String> doBulkQuery(BulkConnection bulkConnection) throws IOException, AsyncApiException, InterruptedException {
    JobInfo job = new JobInfo();
    job.setObject(config.object);
    job.setOperation(OperationEnum.query);
    job.setConcurrencyMode(ConcurrencyMode.Parallel);
    job.setContentType(ContentType.CSV);
    job = bulkConnection.createJob(job);
    Preconditions.checkState(job.getId() != null);
    job = bulkConnection.getJobStatus(job.getId());

    String[] queryResults = null;
    BatchInfo info;
    try (ByteArrayInputStream bout =
           new ByteArrayInputStream(config.query.getBytes())) {
      info = bulkConnection.createBatchFromStream(job, bout);


      for (int i = 0; i < 10000; i++) {
        Thread.sleep(30000); //30 sec
        info = bulkConnection.getBatchInfo(job.getId(),
                                           info.getId());

        if (info.getState() == BatchStateEnum.Completed) {
          QueryResultList list = bulkConnection.getQueryResultList(job.getId(), info.getId());
          queryResults = list.getResult();
          break;
        } else if (info.getState() == BatchStateEnum.Failed) {
          System.out.println("-------------- failed ----------"
                               + info);
          break;
        } else {
          System.out.println("-------------- waiting ----------"
                               + info);
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

  @VisibleForTesting
  AuthResponse oauthLogin() throws Exception {
    SslContextFactory sslContextFactory = new SslContextFactory();
    HttpClient httpClient = new HttpClient(sslContextFactory);
    try {
      httpClient.start();
      String response = httpClient.POST(AUTH_URL).param("grant_type", "password")
        .param("client_id", config.clientId)
        .param("client_secret", config.clientSecret)
        .param("username", config.username)
        .param("password", config.password).send().getContentAsString();
      return GSON.fromJson(response, AuthResponse.class);
    } finally {
      httpClient.stop();
    }
  }

  /**
   * Create the BulkConnection used to call Bulk API operations.
   */
  @VisibleForTesting
  BulkConnection getBulkConnection() throws Exception {
    AuthResponse authResponse = oauthLogin();
    ConnectorConfig connectorConfig = new ConnectorConfig();
    connectorConfig.setSessionId(authResponse.getAccessToken());
    // https://instance_name—api.salesforce.com/services/async/APIversion/job/jobid/batch
    String restEndpoint = String.format("https://%s.salesforce.com/services/async/%s",
                                        config.instance, config.apiVersion);
    connectorConfig.setRestEndpoint(restEndpoint);
    // This should only be false when doing debugging.
    connectorConfig.setCompression(true);
    // Set this to true to see HTTP requests and responses on stdout
    connectorConfig.setTraceMessage(false);
    return new BulkConnection(connectorConfig);
  }

  /**
   * Creates a Bulk API job and uploads batches for a CSV file.
   */
  private void runSample(String sobjectType, String sampleFileName)
    throws Exception {
    BulkConnection connection = getBulkConnection();
    JobInfo job = createJob(sobjectType, connection);
    List<BatchInfo> batchInfoList = createBatchesFromCSVFile(connection, job,
                                                             sampleFileName);
    closeJob(connection, job.getId());
    awaitCompletion(connection, job, batchInfoList);
    checkResults(connection, job, batchInfoList);
  }


  /**
   * Gets the results of the operation and checks for errors.
   */
  private void checkResults(BulkConnection connection, JobInfo job,
                            List<BatchInfo> batchInfoList)
    throws AsyncApiException, IOException {
    // batchInfoList was populated when batches were created and submitted
    for (BatchInfo b : batchInfoList) {
      CSVReader rdr =
        new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()));
      List<String> resultHeader = rdr.nextRecord();
      int resultCols = resultHeader.size();

      List<String> row;
      while ((row = rdr.nextRecord()) != null) {
        Map<String, String> resultInfo = new HashMap<>();
        for (int i = 0; i < resultCols; i++) {
          resultInfo.put(resultHeader.get(i), row.get(i));
        }
        boolean success = Boolean.valueOf(resultInfo.get("Success"));
        boolean created = Boolean.valueOf(resultInfo.get("Created"));
        String id = resultInfo.get("Id");
        String error = resultInfo.get("Error");
        if (success && created) {
          System.out.println("Created row with id " + id);
        } else if (!success) {
          System.out.println("Failed with error: " + error);
        }
      }
    }
  }


  private void closeJob(BulkConnection connection, String jobId) throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setId(jobId);
    job.setState(JobStateEnum.Closed);
    connection.updateJob(job);
  }


  /**
   * Wait for a job to complete by polling the Bulk API.
   *
   * @param connection    BulkConnection used to check results.
   * @param job           The job awaiting completion.
   * @param batchInfoList List of batches for this job.
   * @throws AsyncApiException
   */
  private void awaitCompletion(BulkConnection connection, JobInfo job,
                               List<BatchInfo> batchInfoList) throws AsyncApiException {
    long sleepTime = 0L;
    Set<String> incomplete = new HashSet<>();
    for (BatchInfo bi : batchInfoList) {
      incomplete.add(bi.getId());
    }
    while (!incomplete.isEmpty()) {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
      }
      System.out.println("Awaiting results..." + incomplete.size());
      sleepTime = 10000L;
      BatchInfo[] statusList =
        connection.getBatchInfoList(job.getId()).getBatchInfo();
      for (BatchInfo b : statusList) {
        if (b.getState() == BatchStateEnum.Completed
          || b.getState() == BatchStateEnum.Failed) {
          if (incomplete.remove(b.getId())) {
            System.out.println("BATCH STATUS:\n" + b);
          }
        }
      }
    }
  }


  /**
   * Create a new job using the Bulk API.
   *
   * @param sobjectType The object type being loaded, such as "Account"
   * @param connection  BulkConnection used to create the new job.
   * @return The JobInfo for the new job.
   * @throws AsyncApiException
   */
  private JobInfo createJob(String sobjectType, BulkConnection connection) throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setObject(sobjectType);
    job.setOperation(OperationEnum.query);
    job = connection.createJob(job);
    return job;
  }

  /**
   * Create and upload batches using a CSV file.
   * The file into the appropriate size batch files.
   *
   * @param connection  Connection to use for creating batches
   * @param jobInfo     Job associated with new batches
   * @param csvFileName The source file for batch data
   */
  private List<BatchInfo> createBatchesFromCSVFile(BulkConnection connection,
                                                   JobInfo jobInfo, String csvFileName)
    throws IOException, AsyncApiException {
    List<BatchInfo> batchInfos = new ArrayList<>();
    BufferedReader rdr = new BufferedReader(
      new InputStreamReader(new FileInputStream(csvFileName))
    );
    // read the CSV header row
    byte[] headerBytes = (rdr.readLine() + "\n").getBytes("UTF-8");
    int headerBytesLength = headerBytes.length;
    File tmpFile = File.createTempFile("bulkAPIInsert", ".csv");

    // Split the CSV file into multiple batches
    try {
      FileOutputStream tmpOut = new FileOutputStream(tmpFile);
      int maxBytesPerBatch = 10000000; // 10 million bytes per batch
      int maxRowsPerBatch = 10000; // 10 thousand rows per batch
      int currentBytes = 0;
      int currentLines = 0;
      String nextLine;
      while ((nextLine = rdr.readLine()) != null) {
        byte[] bytes = (nextLine + "\n").getBytes("UTF-8");
        // Create a new batch when our batch size limit is reached
        if (currentBytes + bytes.length > maxBytesPerBatch
          || currentLines > maxRowsPerBatch) {
          createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
          currentBytes = 0;
          currentLines = 0;
        }
        if (currentBytes == 0) {
          tmpOut = new FileOutputStream(tmpFile);
          tmpOut.write(headerBytes);
          currentBytes = headerBytesLength;
          currentLines = 1;
        }
        tmpOut.write(bytes);
        currentBytes += bytes.length;
        currentLines++;
      }
      // Finished processing all rows
      // Create a final batch for any remaining data
      if (currentLines > 1) {
        createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
      }
    } finally {
      tmpFile.delete();
    }
    return batchInfos;
  }

  /**
   * Create a batch by uploading the contents of the file.
   * This closes the output stream.
   *
   * @param tmpOut     The output stream used to write the CSV data for a single batch.
   * @param tmpFile    The file associated with the above stream.
   * @param batchInfos The batch info for the newly created batch is added to this list.
   * @param connection The BulkConnection used to create the new batch.
   * @param jobInfo    The JobInfo associated with the new batch.
   */
  private void createBatch(FileOutputStream tmpOut, File tmpFile,
                           List<BatchInfo> batchInfos, BulkConnection connection, JobInfo jobInfo)
    throws IOException, AsyncApiException {
    tmpOut.flush();
    tmpOut.close();
    try (FileInputStream tmpInputStream = new FileInputStream(tmpFile)) {
      try {
        BatchInfo batchInfo =
          connection.createBatchFromStream(jobInfo, tmpInputStream);
        batchInfos.add(batchInfo);

      } finally {
        tmpInputStream.close();
      }
    }
  }

  @VisibleForTesting
  static final class AuthResponse {
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

    String getAccessToken() {
      return accessToken;
    }

    String getInstanceUrl() {
      return instanceUrl;
    }

    String getId() {
      return id;
    }

    String getTokenType() {
      return tokenType;
    }

    String getIssuedAt() {
      return issuedAt;
    }

    String getSignature() {
      return signature;
    }
  }
}
