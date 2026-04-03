/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadChangeStreamOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link BigtableChangeStreamsToBigtableOptions} class provides the custom execution options
 * passed by the executor at the command-line.
 */
public interface BigtableChangeStreamsToBigtableOptions
    extends DataflowPipelineOptions, ReadChangeStreamOptions {

  @TemplateParameter.Text(
      order = 1,
      regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
      description = "Bigtable Write Instance ID",
      helpText = "The ID of the Bigtable instance to write to.")
  @Validation.Required
  String getBigtableWriteInstanceId();

  void setBigtableWriteInstanceId(String value);

  @TemplateParameter.Text(
      order = 2,
      regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
      description = "Bigtable Write Table ID",
      helpText = "The ID of the Bigtable table to write to.")
  @Validation.Required
  String getBigtableWriteTableId();

  void setBigtableWriteTableId(String value);

  @TemplateParameter.ProjectId(
      order = 3,
      optional = true,
      description = "Bigtable Write Project ID",
      helpText = "The ID of the Google Cloud project that contains the Bigtable instance to write to.")
  String getBigtableWriteProjectId();

  void setBigtableWriteProjectId(String value);

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
      description = "Bigtable Write App Profile",
      helpText =
          "The ID of the Bigtable application profile to use for the export. If you"
              + " do not specify an app profile, Bigtable uses the"
              + " default app profile (https://cloud.google.com/bigtable/docs/app-profiles#default-app-profile)"
              + " of the instance.")
  @Default.String("default")
  String getBigtableWriteAppProfile();

  @TemplateParameter.Integer(
      order = 5,
      optional = true,
      description = "Bigtable's latency target in milliseconds for latency-based throttling",
      helpText = "The latency target of Bigtable in milliseconds for latency-based throttling.")
  Integer getBigtableBulkWriteLatencyTargetMs();

  void setBigtableBulkWriteLatencyTargetMs(Integer value);

  @TemplateParameter.Integer(
      order = 6,
      optional = true,
      description = "The max number of row keys in a Bigtable batch write operation",
      helpText = "The maximum number of row keys in a Bigtable batch write operation.")
  Integer getBigtableBulkWriteMaxRowKeyCount();

  void setBigtableBulkWriteMaxRowKeyCount(Integer value);

  @TemplateParameter.Integer(
      order = 7,
      optional = true,
      description = "The max amount of bytes in a Bigtable batch write operation",
      helpText = "The maximum bytes to include per Bigtable batch write operation.")
  Integer getBigtableBulkWriteMaxRequestSizeBytes();

  void setBigtableBulkWriteMaxRequestSizeBytes(Integer value);

  @TemplateParameter.Boolean(
      order = 8,
      optional = true,
      description = "Enable bulk write flow control",
      helpText =
          "When set to true, enables bulk write flow control which will use"
              + "server's signal to throttle the writes.")
  @Default.Boolean(false)
  Boolean getBigtableBulkWriteFlowControl();

  void setBigtableBulkWriteFlowControl(Boolean enableFlowControl);
}
