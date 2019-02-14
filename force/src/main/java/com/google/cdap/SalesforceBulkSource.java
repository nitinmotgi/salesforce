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
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.plugin.spark.ReferenceStreamingSource;
import com.google.common.annotations.VisibleForTesting;
import com.google.force.ForceConnectorHelper;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(SalesforceBulkSource.NAME)
@Description(SalesforceBulkSource.DESCRIPTION)
public class SalesforceBulkSource extends ReferenceStreamingSource<StructuredRecord> {
  private static Logger LOG = LoggerFactory.getLogger(SalesforceBulkSource.class);
  static final String NAME = "SalesforceBulk";
  static final String DESCRIPTION = "Salesforce Bulk Source";
  private Config config;

  private static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("body", Schema.of(Schema.Type.BYTES))
  );

  public SalesforceBulkSource(Config config) {
    super(config);
    this.config = config;
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) {

    JavaStreamingContext jssc = streamingContext.getSparkStreamingContext();

    //Log.info("Status: " + getStructuredRecord(status).toString());
    return jssc.receiverStream(new ForceReceiver(this.config.timeout, this.config.clientId,
                                                 this.config.clientSecret,
                                                 this.config.username,
                                                 this.config.password,
                                                 this.config.topic,
                                                 this.config.pushEndPoint
    )).map(
      (Function<String, StructuredRecord>) this::getStructuredRecord
    );

  }

  private StructuredRecord getStructuredRecord(String msgs) {
    return StructuredRecord.builder(DEFAULT_SCHEMA)
      .set("ts", System.currentTimeMillis())
      .set("body", msgs).build();
  }

  public static class Config extends ReferencePluginConfig implements Serializable {
    private static final long serialVersionUID = 4218063781902315444L;


    @Name("clientId")
    @Macro
    private String clientId;

    @Name("clientSecret")
    @Macro
    private String clientSecret;

    @Name("topic")
    @Macro
    private String topic;

    @Name("username")
    @Macro
    private String username;

    @Name("password")
    @Macro
    private String password;

    @Name("pushEndPoint")
    @Macro
    private String pushEndPoint;

    @Name("timeout")
    @Macro
    private int timeout;

    @VisibleForTesting
    public Config(String referenceName, String username, String password,
                  String clientId,
                  String clientSecret,
                  String topic,
                  String pushEndPoint,
                  int timeout) {
      super(referenceName);
      this.username = username;
      this.password = password;
      this.clientId = clientId;
      this.clientSecret = clientSecret;
      this.topic = topic;
      this.pushEndPoint = pushEndPoint;
      this.timeout = timeout;
    }
  }

  class ForceReceiver extends Receiver<String> {

    String username;
    String password;
    String clientId;
    String clientSecret;
    String topic;
    String pushEndPoint;
    int timeout;
    ForceConnectorHelper fconnect;

    ForceReceiver(int timeout, String clientId, String clientSecret, String username, String password, String topic, String pushEndPoint) {
      super(StorageLevel.MEMORY_AND_DISK_2());
      this.clientId = clientId;
      this.clientSecret = clientSecret;
      this.username = username;
      this.password = password;
      this.topic = topic;
      this.pushEndPoint = pushEndPoint;
      this.timeout = timeout;
    }

    @Override
    public void onStart() {

      fconnect = new ForceConnectorHelper(
        this.timeout,
        this.clientId,
        this.clientSecret,
        this.username,
        this.password,
        this.topic,
        this.pushEndPoint);

      fconnect.start();
      new Thread("sobject_thread") {
        @Override
        public void run() {
          receive();
        }
      }.start();
    }

    @Override
    public void onStop() {
      // There is nothing much to do as the thread calling receive()
      // is designed to stop by itself if isStopped() returns false
    }

    private void receive() {
      try {
        while (!isStopped()) {
          for (JSONObject jsonObject : fconnect.getMessages()) {
            String event = jsonObject.toString();
            store(event);
            LOG.info("event stored in SparkBlockManager: " + event);
          }
          Thread.sleep(2000);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

}


