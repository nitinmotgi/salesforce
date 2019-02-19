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
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import com.google.cdap.AuthResponse;
import com.google.cdap.BasicSalesforceConfig;
import com.google.common.annotations.VisibleForTesting;
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
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;
import com.sforce.ws.ConnectorConfig;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SalesforceBatchSource.NAME)
@Description("PLugin to read data from Salesforce in batches.")
public class SalesforceBatchSource extends BatchSource<NullWritable, NullWritable, StructuredRecord> {
  static final String NAME = "Salesforce";
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceBatchSource.class);
  private static final String AUTH_URL = "https://login.salesforce.com/services/oauth2/token";
  private static final Gson GSON = new Gson();
  private static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "record",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );

  private final Config config;

  private BulkConnection bulkConnection;
  private JobInfo job;

  @VisibleForTesting
  SalesforceBatchSource(Config config) {
    this.config = config;
  }

  @VisibleForTesting
  static final class Config extends BasicSalesforceConfig {

    @Description("The SOQL query to retrieve results for")
    @Macro
    private final String query;

    Config(String referenceName, String clientId, String clientSecret,
           String username, String password, String object, String query) {
      this(referenceName, clientId, clientSecret, username, password, object, query, "45");
    }

    Config(String referenceName, String clientId, String clientSecret,
           String username, String password, String object, String query, String apiVersion) {
      super(referenceName, clientId, clientSecret, username, password, object, apiVersion);
      this.query = query;
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // validations please
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    setInputFormat(context);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    bulkConnection = getBulkConnection();
    job = createJob();
  }

  @Override
  public void transform(KeyValue<NullWritable, NullWritable> input, Emitter<StructuredRecord> emitter) throws Exception {
    List<String> results = runBulkQuery();
    for (String result : results) {
      for (String eachResult : result.split("\n")) {
        StructuredRecord structuredRecord = StructuredRecord.builder(DEFAULT_SCHEMA)
          .set("ts", System.currentTimeMillis())
          .set("body", eachResult)
          .build();
        emitter.emit(structuredRecord);
      }
    }
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    job.setState(JobStateEnum.Closed);
    try {
      bulkConnection.updateJob(job);
    } catch (AsyncApiException e) {
      LOG.warn("Unable to successfully close job %s. Ignoring", job.getId());
    }
  }

  /**
   * Create a new job using the Bulk API.
   *
   * @return The JobInfo for the new job.
   * @throws AsyncApiException if there is an issue creating the job
   */
  private JobInfo createJob() throws AsyncApiException {
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

  @VisibleForTesting
  List<String> runBulkQuery() throws IOException, AsyncApiException, InterruptedException {
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
   * Create the BulkConnection used to call Bulk API operations.
   */
  private BulkConnection getBulkConnection() throws Exception {
    AuthResponse authResponse = oauthLogin();
    ConnectorConfig connectorConfig = new ConnectorConfig();
    connectorConfig.setSessionId(authResponse.getAccessToken());
    // https://instance_name—api.salesforce.com/services/async/APIversion/job/jobid/batch
    String restEndpoint = String.format("https://%s/services/async/%s",
                                        authResponse.getInstanceUrl(), config.getApiVersion());
    connectorConfig.setRestEndpoint(restEndpoint);
    // This should only be false when doing debugging.
    connectorConfig.setCompression(true);
    // Set this to true to see HTTP requests and responses on stdout
    connectorConfig.setTraceMessage(false);
    return new BulkConnection(connectorConfig);
  }

  private void setInputFormat(BatchSourceContext context) {
    context.setInput(Input.of(config.referenceName, new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return NoOpInputFormat.class.getName();
      }

      @Override
      public Map<String, String> getInputFormatConfiguration() {
        return Collections.EMPTY_MAP;
      }
    }));
  }

//  /**
//   * Creates a Bulk API job and uploads batches for a CSV file.
//   */
//  private void runSample(String sampleFileName)
//    throws Exception {
//    BulkConnection connection = getBulkConnection();
//    JobInfo job = createJob();
//    List<BatchInfo> batchInfoList = createBatchesFromCSVFile(connection, job, sampleFileName);
//    closeJob();
//    awaitCompletion(connection, job, batchInfoList);
//    checkResults(connection, job, batchInfoList);
//  }


  /**
   * Gets the results of the operation and checks for errors.
   */
//  private void checkResults(BulkConnection connection, JobInfo job,
//                            List<BatchInfo> batchInfoList)
//    throws AsyncApiException, IOException {
//    // batchInfoList was populated when batches were created and submitted
//    for (BatchInfo b : batchInfoList) {
//      CSVReader rdr =
//        new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()));
//      List<String> resultHeader = rdr.nextRecord();
//      int resultCols = resultHeader.size();
//
//      List<String> row;
//      while ((row = rdr.nextRecord()) != null) {
//        Map<String, String> resultInfo = new HashMap<>();
//        for (int i = 0; i < resultCols; i++) {
//          resultInfo.put(resultHeader.get(i), row.get(i));
//        }
//        boolean success = Boolean.valueOf(resultInfo.get("Success"));
//        boolean created = Boolean.valueOf(resultInfo.get("Created"));
//        String id = resultInfo.get("Id");
//        String error = resultInfo.get("Error");
//        if (success && created) {
//          System.out.println("Created row with id " + id);
//        } else if (!success) {
//          System.out.println("Failed with error: " + error);
//        }
//      }
//    }
//  }
//
//  /**
//   * Wait for a job to complete by polling the Bulk API.
//   *
//   * @param connection    BulkConnection used to check results.
//   * @param job           The job awaiting completion.
//   * @param batchInfoList List of batches for this job.
//   * @throws AsyncApiException
//   */
//  private void awaitCompletion(BulkConnection connection, JobInfo job,
//                               List<BatchInfo> batchInfoList) throws AsyncApiException {
//    long sleepTime = 0L;
//    Set<String> incomplete = new HashSet<>();
//    for (BatchInfo bi : batchInfoList) {
//      incomplete.add(bi.getId());
//    }
//    while (!incomplete.isEmpty()) {
//      try {
//        Thread.sleep(sleepTime);
//      } catch (InterruptedException e) {
//      }
//      System.out.println("Awaiting results..." + incomplete.size());
//      sleepTime = 10000L;
//      BatchInfo[] statusList =
//        connection.getBatchInfoList(job.getId()).getBatchInfo();
//      for (BatchInfo b : statusList) {
//        if (b.getState() == BatchStateEnum.Completed
//          || b.getState() == BatchStateEnum.Failed) {
//          if (incomplete.remove(b.getId())) {
//            System.out.println("BATCH STATUS:\n" + b);
//          }
//        }
//      }
//    }
//  }

//  /**
//   * Create and upload batches using a CSV file.
//   * The file into the appropriate size batch files.
//   *
//   * @param connection  Connection to use for creating batches
//   * @param jobInfo     Job associated with new batches
//   * @param csvFileName The source file for batch data
//   */
//  private List<BatchInfo> createBatchesFromCSVFile(BulkConnection connection,
//                                                   JobInfo jobInfo, String csvFileName)
//    throws IOException, AsyncApiException {
//    List<BatchInfo> batchInfos = new ArrayList<>();
//    BufferedReader rdr = new BufferedReader(
//      new InputStreamReader(new FileInputStream(csvFileName))
//    );
//    // read the CSV header row
//    byte[] headerBytes = (rdr.readLine() + "\n").getBytes("UTF-8");
//    int headerBytesLength = headerBytes.length;
//    File tmpFile = File.createTempFile("bulkAPIInsert", ".csv");
//
//    // Split the CSV file into multiple batches
//    try {
//      FileOutputStream tmpOut = new FileOutputStream(tmpFile);
//      int maxBytesPerBatch = 10000000; // 10 million bytes per batch
//      int maxRowsPerBatch = 10000; // 10 thousand rows per batch
//      int currentBytes = 0;
//      int currentLines = 0;
//      String nextLine;
//      while ((nextLine = rdr.readLine()) != null) {
//        byte[] bytes = (nextLine + "\n").getBytes("UTF-8");
//        // Create a new batch when our batch size limit is reached
//        if (currentBytes + bytes.length > maxBytesPerBatch
//          || currentLines > maxRowsPerBatch) {
//          createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
//          currentBytes = 0;
//          currentLines = 0;
//        }
//        if (currentBytes == 0) {
//          tmpOut = new FileOutputStream(tmpFile);
//          tmpOut.write(headerBytes);
//          currentBytes = headerBytesLength;
//          currentLines = 1;
//        }
//        tmpOut.write(bytes);
//        currentBytes += bytes.length;
//        currentLines++;
//      }
//      // Finished processing all rows
//      // Create a final batch for any remaining data
//      if (currentLines > 1) {
//        createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
//      }
//    } finally {
//      tmpFile.delete();
//    }
//    return batchInfos;
//  }

//  /**
//   * Create a batch by uploading the contents of the file.
//   * This closes the output stream.
//   *
//   * @param tmpOut     The output stream used to write the CSV data for a single batch.
//   * @param tmpFile    The file associated with the above stream.
//   * @param batchInfos The batch info for the newly created batch is added to this list.
//   * @param connection The BulkConnection used to create the new batch.
//   * @param jobInfo    The JobInfo associated with the new batch.
//   */
//  private void createBatch(FileOutputStream tmpOut, File tmpFile,
//                           List<BatchInfo> batchInfos, BulkConnection connection, JobInfo jobInfo)
//    throws IOException, AsyncApiException {
//    tmpOut.flush();
//    tmpOut.close();
//    try (FileInputStream tmpInputStream = new FileInputStream(tmpFile)) {
//      try {
//        BatchInfo batchInfo =
//          connection.createBatchFromStream(jobInfo, tmpInputStream);
//        batchInfos.add(batchInfo);
//
//      } finally {
//        tmpInputStream.close();
//      }
//    }
//  }

  static final class NoOpInputFormat extends InputFormat {
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) {
      return Collections.emptyList();
    }

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
      return new RecordReader() {
        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {

        }

        @Override
        public boolean nextKeyValue() {
          return false;
        }

        @Override
        public Object getCurrentKey() {
          return null;
        }

        @Override
        public Object getCurrentValue() {
          return null;
        }

        @Override
        public float getProgress() {
          return 0;
        }

        @Override
        public void close() {

        }
      };
    }
  }
}
