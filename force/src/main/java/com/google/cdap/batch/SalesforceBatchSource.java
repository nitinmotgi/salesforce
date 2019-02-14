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

import java.io.*;
import java.util.*;

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.ws.ConnectorConfig;


public class SalesforceBatchSource extends BatchSource {


  public static void main(String[] args)
    throws AsyncApiException, IOException {
    SalesforceBatchSource example = new SalesforceBatchSource();
    // Replace arguments below with your credentials and test file name
    // The first parameter indicates that we are loading Account records
    example.runSample("Account", "myUser@myOrg.com", "myPassword", "mySampleData.csv");
  }

  /**
   * Creates a Bulk API job and uploads batches for a CSV file.
   */
  private void runSample(String sobjectType, String userName,
                        String password, String sampleFileName)
    throws AsyncApiException, IOException {
    BulkConnection connection = getBulkConnection(userName, password);
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


  private void closeJob(BulkConnection connection, String jobId)
    throws AsyncApiException {
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
                               List<BatchInfo> batchInfoList)
    throws AsyncApiException {
    long sleepTime = 0L;
    Set<String> incomplete = new HashSet<String>();
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
  private JobInfo createJob(String sobjectType, BulkConnection connection)
    throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setObject(sobjectType);
    job.setOperation(OperationEnum.insert);
    job.setContentType(ContentType.CSV);
    job = connection.createJob(job);
    System.out.println(job);
    return job;
  }


  /**
   * Create the BulkConnection used to call Bulk API operations.
   */
  private BulkConnection getBulkConnection(String userName, String password) throws AsyncApiException {
    ConnectorConfig partnerConfig = new ConnectorConfig();
    partnerConfig.setUsername(userName);
    partnerConfig.setPassword(password);
    partnerConfig.setAuthEndpoint("https://login.salesforce.com/services/Soap/u/45.0");
    // Creating the connection automatically handles login and stores
    // the session in partnerConfig
    // new PartnerConnection(partnerConfig);
    // When PartnerConnection is instantiated, a login is implicitly
    // executed and, if successful,
    // a valid session is stored in the ConnectorConfig instance.
    // Use this key to initialize a BulkConnection:
    ConnectorConfig config = new ConnectorConfig();
    config.setSessionId(partnerConfig.getSessionId());
    // The endpoint for the Bulk API service is the same as for the normal
    // SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
    String soapEndpoint = partnerConfig.getServiceEndpoint();
    String apiVersion = "45.0";
    String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/"))
      + "async/" + apiVersion;
    config.setRestEndpoint(restEndpoint);
    // This should only be false when doing debugging.
    config.setCompression(true);
    // Set this to true to see HTTP requests and responses on stdout
    config.setTraceMessage(false);
    return new BulkConnection(config);
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
    List<BatchInfo> batchInfos = new ArrayList<BatchInfo>();
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


  @Override
  public void initialize(Object o) throws Exception {

  }

  @Override
  public void prepareRun(BatchContext batchContext) throws Exception {

  }

  @Override
  public void prepareRun(Object o) throws Exception {

  }

  @Override
  public void onRunFinish(boolean b, Object o) {

  }

  @Override
  public void transform(Object o, Emitter emitter) throws Exception {

  }
}