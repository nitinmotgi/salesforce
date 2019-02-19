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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

@Plugin(type = Action.PLUGIN_TYPE)
@Name(SalesforceToGCSAction.NAME)
@Description("Downloads Salesforce data to GCS based on the provided query")
public class SalesforceToGCSAction extends Action {
  static final String NAME = "SalesforceToGCS";
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceToGCSAction.class);
  private static final Gson GSON = new Gson();
  private static final String AUTH_URL = "https://login.salesforce.com/services/oauth2/token";

  private final Config config;

  SalesforceToGCSAction(Config config) {
    this.config = config;
  }

  static final class Config extends BasicSalesforceConfig {
    static final String AUTO_DETECT = "auto-detect";

    @Description("The SOQL query to retrieve results for")
    @Macro
    private String query;

    @Description("Google Cloud Project ID, which uniquely identifies a project. "
      + "It can be found on the Dashboard in the Google Cloud Platform Console.")
    @Macro
    @Nullable
    private String project;

    @Description("Path on the local file system of the service account key used "
      + "for authorization. Can be set to 'auto-detect' when running on a Dataproc cluster. "
      + "When running on other clusters, the file must be present on every node in the cluster.")
    @Macro
    @Nullable
    private String serviceAccountPath;

    @Description("The GCS bucket to upload data to. If it does not exist, a new bucket will be created.")
    @Macro
    private String bucket;

    @Description("The path to the file to create in the specified bucket.")
    @Macro
    private String subPath;

    Config() {
      super();
      project = AUTO_DETECT;
      serviceAccountPath = AUTO_DETECT;
    }

    Config(String referenceName, String clientId, String clientSecret, String username, String password,
           String instance, String object, String query, @Nullable String project, @Nullable String serviceAccountPath,
           String bucket, String subPath, @Nullable String apiVersion) {
      super(referenceName, clientId, clientSecret, username, password, instance, object, apiVersion);
      this.query = query;
      this.project = project;
      this.serviceAccountPath = serviceAccountPath;
      this.bucket = bucket;
      this.subPath = subPath;
    }

    public String getProject() {
      String projectId = project;
      if (project == null || project.isEmpty() || AUTO_DETECT.equals(project)) {
        projectId = ServiceOptions.getDefaultProjectId();
      }
      if (projectId == null) {
        throw new IllegalArgumentException(
          "Could not detect Google Cloud project id from the environment. Please specify a project id.");
      }
      return projectId;
    }

    @Nullable
    public String getServiceAccountFilePath() {
      if (containsMacro("serviceAccountPath") || serviceAccountPath == null ||
        serviceAccountPath.isEmpty() || AUTO_DETECT.equals(serviceAccountPath)) {
        return null;
      }
      return serviceAccountPath;
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void run(ActionContext actionContext) throws Exception {
    BulkConnection bulkConnection = getBulkConnection();
    JobInfo job = createJob(bulkConnection);
    List<String> results = runBulkQuery(bulkConnection, job);
    for (String result : results) {
      write1(result);
    }
  }

  private void write1(String data) throws IOException, GeneralSecurityException {
    InputStream is = new ByteArrayInputStream(data.getBytes(Charsets.UTF_8));
    InputStreamContent contentStream = new InputStreamContent("application/text", is);
    // Setting the length improves upload performance
    contentStream.setLength(data.length());
    StorageObject objectMetadata = new StorageObject()
      // Set the destination object name
      .setName(config.subPath);

    // Do the insert
    com.google.api.services.storage.Storage client = createService();
    com.google.api.services.storage.Storage.Objects.Insert insertRequest =
      client.objects().insert(config.bucket, objectMetadata, contentStream);

    insertRequest.execute();

  }

  private com.google.api.services.storage.Storage createService() throws IOException, GeneralSecurityException {
    HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    GoogleCredential credential = GoogleCredential.fromStream( new FileInputStream(new File(config.serviceAccountPath)));

    // Depending on the environment that provides the default credentials (for
    // example: Compute Engine, App Engine), the credentials may require us to
    // specify the scopes we need explicitly.  Check for this case, and inject
    // the Cloud Storage scope if required.
    if (credential.createScopedRequired()) {
      Collection<String> scopes = StorageScopes.all();
      credential = credential.createScoped(scopes);
    }

    return new com.google.api.services.storage.Storage.Builder(transport, jsonFactory, credential)
      .setApplicationName("GCS Samples")
      .build();
  }


  private void write(String data) throws IOException {
    Storage storage = createStorage();
    BlobInfo blob = BlobInfo.newBuilder(config.bucket, config.subPath).setContentType("application/text").build();
    WriteChannel writer = storage.writer(blob);
    writer.write(ByteBuffer.wrap(data.getBytes(Charsets.UTF_8)));
  }

  private Storage createStorage() throws IOException {
    StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(config.project);
    if (config.serviceAccountPath != null) {
      builder.setCredentials(loadServiceAccountCredentials(config.serviceAccountPath));
    }
    return builder.build().getService();
  }

  /**
   * Create the BulkConnection used to call Bulk API operations.
   */
  private BulkConnection getBulkConnection() throws Exception {
    AuthResponse authResponse = oauthLogin();
    ConnectorConfig connectorConfig = new ConnectorConfig();
    connectorConfig.setSessionId(authResponse.getAccessToken());
    // https://instance_name—api.salesforce.com/services/async/APIversion/job/jobid/batch
    String restEndpoint = String.format("https://%s.salesforce.com/services/async/%s",
                                        config.getInstance(), config.getApiVersion());
    connectorConfig.setRestEndpoint(restEndpoint);
    // This should only be false when doing debugging.
    connectorConfig.setCompression(true);
    // Set this to true to see HTTP requests and responses on stdout
    connectorConfig.setTraceMessage(false);
    return new BulkConnection(connectorConfig);
  }

  private AuthResponse oauthLogin() throws Exception {
    SslContextFactory sslContextFactory = new SslContextFactory();
    HttpClient httpClient = new HttpClient(sslContextFactory);
    try {
      httpClient.start();
      String response = httpClient.POST(AUTH_URL).param("grant_type", "password")
        .param("client_id", config.getClientId())
        .param("client_secret", config.getClientSecret())
        .param("username", config.getUsername())
        .param("password", config.getPassword()).send().getContentAsString();
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
  private JobInfo createJob(BulkConnection bulkConnection) throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setObject(config.getObject());
    job.setOperation(OperationEnum.query);
    job.setConcurrencyMode(ConcurrencyMode.Parallel);
    job.setContentType(ContentType.CSV);
    job = bulkConnection.createJob(job);
    Preconditions.checkState(job.getId() != null, "Couldn't get job ID. Perhaps there was a problem in creating the " +
      "batch job");
    return bulkConnection.getJobStatus(job.getId());
  }

  private List<String> runBulkQuery(BulkConnection bulkConnection,
                                    JobInfo job) throws IOException, AsyncApiException, InterruptedException {
    String[] queryResults = null;
    BatchInfo info;
    try (ByteArrayInputStream bout = new ByteArrayInputStream(config.query.getBytes())) {
      info = bulkConnection.createBatchFromStream(job, bout);

      for (int i = 0; i < 10000; i++) {
        Thread.sleep(30000); //30 sec
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

  private ServiceAccountCredentials loadServiceAccountCredentials(String path) throws IOException {
    File credentialsPath = new File(path);
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      return ServiceAccountCredentials.fromStream(serviceAccountStream);
    }
  }
}
