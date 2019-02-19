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

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Arguments;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import org.junit.Test;

import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public class SalesforceBatchSourceTest {

  @Test
  public void test() throws Exception {
    String clientId = "3MVG9KsVczVNcM8zBClG9yeHWyo7gnrm4fvBLZ2kboPV6lKyYJmMogVfAcQ4i.gAR5CixqoJxiT8H1wnnobUb";
    String clientSecret = "129FFD7155B0837F615F1D18CB997F1F43B4D4B29E4D20AE3F3AF0D2E2B06097";
    String username = "bhooshan@cask.co";
    String password = "bdm@SF123";
    String instance = "na85";
    String object = "Account";
    String query = "SELECT Name from Account";
    SalesforceBatchSource sfsource = new SalesforceBatchSource(
      new SalesforceBatchSource.Config("input", clientId, clientSecret, username, password, instance, object, query)
    );

    sfsource.initialize(new BatchJoinerRuntimeContext() {
      @Override
      public long getLogicalStartTime() {
        return 0;
      }

      @Override
      public <T extends Dataset> T getDataset(String s) throws DatasetInstantiationException {
        return null;
      }

      @Override
      public <T extends Dataset> T getDataset(String s, String s1) throws DatasetInstantiationException {
        return null;
      }

      @Override
      public <T extends Dataset> T getDataset(String s, Map<String, String> map) throws DatasetInstantiationException {
        return null;
      }

      @Override
      public <T extends Dataset> T getDataset(String s, String s1, Map<String, String> map) throws DatasetInstantiationException {
        return null;
      }

      @Override
      public void releaseDataset(Dataset dataset) {

      }

      @Override
      public void discardDataset(Dataset dataset) {

      }

      @Override
      public <T> Lookup<T> provide(String s, Map<String, String> map) {
        return null;
      }

      @Override
      public String getStageName() {
        return null;
      }

      @Override
      public String getNamespace() {
        return null;
      }

      @Override
      public String getPipelineName() {
        return null;
      }

      @Override
      public StageMetrics getMetrics() {
        return null;
      }

      @Override
      public PluginProperties getPluginProperties() {
        return null;
      }

      @Override
      public PluginProperties getPluginProperties(String s) {
        return null;
      }

      @Override
      public <T> Class<T> loadPluginClass(String s) {
        return null;
      }

      @Override
      public <T> T newPluginInstance(String s) {
        return null;
      }

      @Nullable
      @Override
      public Schema getInputSchema() {
        return null;
      }

      @Override
      public Map<String, Schema> getInputSchemas() {
        return null;
      }

      @Nullable
      @Override
      public Schema getOutputSchema() {
        return null;
      }

      @Override
      public Map<String, Schema> getOutputPortSchemas() {
        return null;
      }

      @Override
      public Arguments getArguments() {
        return null;
      }

      @Nullable
      @Override
      public URL getServiceURL(String s, String s1) {
        return null;
      }

      @Nullable
      @Override
      public URL getServiceURL(String s) {
        return null;
      }

      @Override
      public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) {
        return null;
      }

      @Override
      public Metadata getMetadata(MetadataScope metadataScope, MetadataEntity metadataEntity) {
        return null;
      }

      @Override
      public void addProperties(MetadataEntity metadataEntity, Map<String, String> map) {

      }

      @Override
      public void addTags(MetadataEntity metadataEntity, String... strings) {

      }

      @Override
      public void addTags(MetadataEntity metadataEntity, Iterable<String> iterable) {

      }

      @Override
      public void removeMetadata(MetadataEntity metadataEntity) {

      }

      @Override
      public void removeProperties(MetadataEntity metadataEntity) {

      }

      @Override
      public void removeProperties(MetadataEntity metadataEntity, String... strings) {

      }

      @Override
      public void removeTags(MetadataEntity metadataEntity) {

      }

      @Override
      public void removeTags(MetadataEntity metadataEntity, String... strings) {

      }

      @Override
      public void record(List<FieldOperation> list) {

      }
    });
    System.out.println(sfsource.runBulkQuery());
  }
}
