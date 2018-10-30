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


import com.sforce.async.BatchInfo;
import com.sforce.async.BulkConnection;
import com.sforce.async.JobInfo;
import com.sforce.async.QueryResultList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BulkExtract {
  private static final Logger LOG = LoggerFactory.getLogger(BulkExtract.class);

  private BulkConnection connection;
  private BatchInfo batch;
  private JobInfo info;
  private QueryResultList queryResultList;
  private PartnerConfig partnerConfig;


}
