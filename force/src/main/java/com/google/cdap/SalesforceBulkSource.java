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
import com.google.force.ForceReceiver;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.api.java.function.Function;
import java.io.Serializable;


@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(SalesforceBulkSource.NAME)
@Description(SalesforceBulkSource.DESCRIPTION)
public class SalesforceBulkSource extends ReferenceStreamingSource<StructuredRecord> {
    public static final String NAME = "SalesforceBulk";
    public static final String DESCRIPTION = "Salesforce Bulk Source";
    private Config config;
    ForceConnectorHelper fconnect;

    private static final Schema DEFAULT_SCHEMA = Schema.recordOf(
            "event",
            Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
            Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
            Schema.Field.of("body", Schema.of(Schema.Type.BYTES))
    );

    public SalesforceBulkSource(Config config) {
        super(config);
        this.config = config;
    }

    @Override
    public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {

        JavaStreamingContext jssc = streamingContext.getSparkStreamingContext();

        return jssc.receiverStream(new ForceReceiver(10,this.config.clientId,
                this.config.clientSecret,
                this.config.username,
                this.config.password,
                this.config.topic)).map(
                new Function<String, StructuredRecord>() {
                    public StructuredRecord call(String status) {
                        return getStructuredRecord(status);
                    }
                }
        );

    }
    private StructuredRecord getStructuredRecord(String msgs) {
            return StructuredRecord.builder(DEFAULT_SCHEMA)
                    .set("ts",System.currentTimeMillis())
                    .set("headers","someheader")
                    .set("body",msgs).build();
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

        @Name("authendpoint")
        @Macro
        private String authEndpoint;

        @Name("version")
        @Macro
        private String version;

        @Name("incremental")
        @Macro
        private String incremental;

        @Name("sql")
        @Macro
        private String sql;

        @Name("offset")
        @Macro
        private long offset;

        @Name("field")
        @Macro
        private String field;

        @Name("query-interval-min")
        @Macro
        private int queryIntervalInMin;

        @VisibleForTesting
        public Config(String referenceName, String username, String password,
                      String authEndpoint, String version, String incremental,
                      String sql, long offset, String field, int queryIntervalInMin, String clientId,
                      String clientSecret,
                      String topic) {
            super(referenceName);
            this.username = username;
            this.password = password;
            this.clientId = clientId;
            this.clientSecret = clientSecret;
            this.topic = topic;
            this.authEndpoint = authEndpoint;
            this.version = version;
            this.incremental = incremental;
            this.sql = sql;
            this.offset = offset;
            this.field = field;
            this.queryIntervalInMin = queryIntervalInMin;
        }
    }

}
