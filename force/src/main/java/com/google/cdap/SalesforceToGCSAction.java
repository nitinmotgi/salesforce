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
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Charsets;
import com.sforce.async.BulkConnection;
import com.sforce.async.JobInfo;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

@Plugin(type = Action.PLUGIN_TYPE)
@Name(SalesforceToGCSAction.NAME)
@Description("Downloads Salesforce data to GCS based on the provided query")
public class SalesforceToGCSAction extends Action {
  static final String NAME = "SalesforceToGCS";

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
           String object, String query, @Nullable String project, @Nullable String serviceAccountPath,
           String bucket, String subPath, @Nullable String apiVersion) {
      super(referenceName, clientId, clientSecret, username, password, object, apiVersion);
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
    String getServiceAccountFilePath() {
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
    BulkConnection bulkConnection = SalesforceBulkAPIs.getBulkConnection(config.getClientId(), config.getClientSecret(),
                                                                         config.getUsername(), config.getPassword(),
                                                                         config.getApiVersion());
    JobInfo job = SalesforceBulkAPIs.createJob(config.getObject(), bulkConnection);
    List<String> results = SalesforceBulkAPIs.runBulkQuery(config.query, bulkConnection, job);
    for (String result : results) {
      write(result);
    }
  }

  private void write(String data) throws IOException, GeneralSecurityException {
    InputStream is = new ByteArrayInputStream(data.getBytes(Charsets.UTF_8));
    InputStreamContent contentStream = new InputStreamContent("application/text", is);
    // Setting the length improves upload performance
    contentStream.setLength(data.length());
    StorageObject objectMetadata = new StorageObject()
      // Set the destination object name
      .setName(config.subPath);

    // Do the insert
    Storage client = createStorage();
    Storage.Objects.Insert insertRequest = client.objects().insert(config.bucket, objectMetadata, contentStream);

    insertRequest.execute();
  }

  private Storage createStorage() throws IOException, GeneralSecurityException {
    HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    GoogleCredential credential = config.getServiceAccountFilePath() != null ?
      GoogleCredential.fromStream(new FileInputStream(new File(config.getServiceAccountFilePath()))) :
      GoogleCredential.getApplicationDefault(transport, jsonFactory);

    // Depending on the environment that provides the default credentials (for
    // example: Compute Engine, App Engine), the credentials may require us to
    // specify the scopes we need explicitly.  Check for this case, and inject
    // the Cloud Storage scope if required.
    if (credential.createScopedRequired()) {
      Collection<String> scopes = StorageScopes.all();
      credential = credential.createScoped(scopes);
    }

//    StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(config.getProject());
//    if (config.getServiceAccountFilePath() != null) {
//      builder.setCredentials(GCPUtils.loadServiceAccountCredentials(serviceAccountPath));
//    }
//    Storage storage = builder.build().getService();
    return new Storage.Builder(transport, jsonFactory, credential)
      .setApplicationName("GCS Samples")
      .build();
  }
}
