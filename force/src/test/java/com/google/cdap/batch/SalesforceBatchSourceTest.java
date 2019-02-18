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

import org.junit.Test;

public class SalesforceBatchSourceTest {
  private static final String TOKEN =
    "6Cel800D1U0000010Ct88881U000001cX7sTvUbPvnJzjuzlcrD7xUTcsomWMYVObx0FGhTj4Uv958teCCStgDUvrDuqhFgIrlA1quDDaKt";

  @Test
  public void test() throws Exception {
    String clientId = "3MVG9KsVczVNcM8zBClG9yeHWyo7gnrm4fvBLZ2kboPV6lKyYJmMogVfAcQ4i.gAR5CixqoJxiT8H1wnnobUb";
    String clientSecret = "129FFD7155B0837F615F1D18CB997F1F43B4D4B29E4D20AE3F3AF0D2E2B06097";
    String username = "bhooshan@cask.co";
    String password = "bdm@SF123";
    SalesforceBatchSource sfsource = new SalesforceBatchSource(new SalesforceBatchSource.Config("input", clientId, clientSecret, username, password));
    System.out.println(sfsource.oauthLogin());
  }
}
