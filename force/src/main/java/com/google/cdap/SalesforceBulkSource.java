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
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.plugin.spark.ReferenceStreamingSource;
import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.Serializable;

@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(SalesforceBulkSource.NAME)
@Description(SalesforceBulkSource.DESCRIPTION)
public class SalesforceBulkSource extends ReferenceStreamingSource<StructuredRecord> {
  public static final String NAME = "SalesforceBulk";
  public static final String DESCRIPTION = "Salesforce Bulk Source";
  private Config config;

  public SalesforceBulkSource(Config config) {
    super(config);
    this.config = config;
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {
    return null;
  }

  public static class Config extends ReferencePluginConfig implements Serializable {
    private static final long serialVersionUID = 4218063781902315444L;

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
                                      String sql, long offset, String field, int queryIntervalInMin) {
      super(referenceName);
      this.username = username;
      this.password = password;
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
