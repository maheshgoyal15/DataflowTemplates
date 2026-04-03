/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigtable;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.TimestampRange;
import com.google.cloud.Timestamp;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadChangeStreamOptions;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToBigtableOptions;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.ExistingPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates that the Bigtable Change Streams to Bigtable template can run.
 */
@Template(
    name = "Bigtable_Change_Streams_to_Bigtable",
    category = TemplateCategory.STREAMING,
    displayName = "Cloud Bigtable Change Streams to Bigtable",
    description =
        "Streaming pipeline. Streams Bigtable data change records and writes them into another Bigtable table using Dataflow Runner V2.",
    optionsClass = BigtableChangeStreamsToBigtableOptions.class,
    optionsOrder = {
      BigtableChangeStreamsToBigtableOptions.class,
      ReadChangeStreamOptions.class
    },
    skipOptions = {
      "bigtableReadAppProfile",
      "bigtableAdditionalRetryCodes",
      "bigtableRpcAttemptTimeoutMs",
      "bigtableRpcTimeoutMs"
    },
    flexContainerName = "googlecloud-to-googlecloud",
    contactInformation = "https://cloud.google.com/support",
    streaming = true)
public final class BigtableChangeStreamsToBigtable {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamsToBigtable.class);

  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    LOG.info("Starting to replicate change records from Cloud Bigtable change streams to Bigtable");

    BigtableChangeStreamsToBigtableOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigtableChangeStreamsToBigtableOptions.class);

    run(options);
  }

  private static void setOptions(BigtableChangeStreamsToBigtableOptions options) {
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    // Add use_runner_v2 to the experiments option, since change streams connector is only supported
    // on Dataflow runner v2.
    List<String> experiments = options.getExperiments();
    if (experiments == null) {
      experiments = new ArrayList<>();
    }
    boolean hasUseRunnerV2 = false;
    for (String experiment : experiments) {
      if (experiment.equalsIgnoreCase(USE_RUNNER_V2_EXPERIMENT)) {
        hasUseRunnerV2 = true;
        break;
      }
    }
    if (!hasUseRunnerV2) {
      experiments.add(USE_RUNNER_V2_EXPERIMENT);
    }
    options.setExperiments(experiments);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(BigtableChangeStreamsToBigtableOptions options) {
    setOptions(options);

    String bigtableProject = getBigtableProjectId(options);
    String bigtableWriteProject = getBigtableWriteProjectId(options);

    // Retrieve and parse the startTimestamp
    Instant startTimestamp =
        options.getBigtableChangeStreamStartTimestamp().isEmpty()
            ? Instant.now()
            : toInstant(Timestamp.parseTimestamp(options.getBigtableChangeStreamStartTimestamp()));

    Pipeline pipeline = Pipeline.create(options);

    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withChangeStreamName(options.getBigtableChangeStreamName())
            .withExistingPipelineOptions(
                options.getBigtableChangeStreamResume()
                    ? ExistingPipelineOptions.RESUME_OR_FAIL
                    : ExistingPipelineOptions.FAIL_IF_EXISTS)
            .withProjectId(bigtableProject)
            .withMetadataTableInstanceId(options.getBigtableChangeStreamMetadataInstanceId())
            .withInstanceId(options.getBigtableReadInstanceId())
            .withTableId(options.getBigtableReadTableId())
            .withAppProfileId(options.getBigtableChangeStreamAppProfile())
            .withStartTime(startTimestamp);

    if (!StringUtils.isBlank(options.getBigtableChangeStreamMetadataTableTableId())) {
      readChangeStream =
          readChangeStream.withMetadataTableTableId(
              options.getBigtableChangeStreamMetadataTableTableId());
    }

    PCollection<ChangeStreamMutation> dataChangeRecord =
        pipeline
            .apply("Read from Cloud Bigtable Change Streams", readChangeStream)
            .apply(Values.create());

    PCollection<KV<ByteString, Iterable<Mutation>>> mutations =
        dataChangeRecord.apply(
            "ChangeStreamMutation To Bigtable Mutation",
            ParDo.of(new ChangeStreamMutationToBigtableMutationFn()));

    BigtableIO.Write write =
        BigtableIO.write()
            .withProjectId(bigtableWriteProject)
            .withInstanceId(options.getBigtableWriteInstanceId())
            .withTableId(options.getBigtableWriteTableId())
            .withAppProfileId(options.getBigtableWriteAppProfile());

    if (options.getBigtableBulkWriteMaxRowKeyCount() != null) {
      write = write.withBatchElements(options.getBigtableBulkWriteMaxRowKeyCount());
    }
    if (options.getBigtableBulkWriteMaxRequestSizeBytes() != null) {
      write = write.withBatchSizeBytes(options.getBigtableBulkWriteMaxRequestSizeBytes());
    }
    if (options.getBigtableBulkWriteFlowControl() != null
        && options.getBigtableBulkWriteFlowControl()) {
      write = write.withFlowControl(true);
    }

    mutations.apply("Write To Bigtable", write);

    return pipeline.run();
  }

  private static Instant toInstant(Timestamp timestamp) {
    if (timestamp == null) {
      return null;
    } else {
      return Instant.ofEpochMilli(timestamp.getSeconds() * 1000 + timestamp.getNanos() / 1000000);
    }
  }

  private static String getBigtableProjectId(BigtableChangeStreamsToBigtableOptions options) {
    return StringUtils.isEmpty(options.getBigtableReadProjectId())
        ? options.getProject()
        : options.getBigtableReadProjectId();
  }

  private static String getBigtableWriteProjectId(BigtableChangeStreamsToBigtableOptions options) {
    return StringUtils.isEmpty(options.getBigtableWriteProjectId())
        ? options.getProject()
        : options.getBigtableWriteProjectId();
  }

  /**
   * DoFn that converts a {@link ChangeStreamMutation} to {@link Mutation}.
   */
  static class ChangeStreamMutationToBigtableMutationFn
      extends DoFn<ChangeStreamMutation, KV<ByteString, Iterable<Mutation>>> {

    @ProcessElement
    public void process(@Element ChangeStreamMutation input, OutputReceiver<KV<ByteString, Iterable<Mutation>>> receiver)
        throws Exception {
      
      List<Mutation> mutations = entriesToMutations(input.getEntries());
      
      if (!mutations.isEmpty()) {
        receiver.output(KV.of(input.getRowKey(), mutations));
      }
    }

    static List<Mutation> entriesToMutations(List<Entry> entries) {
      List<Mutation> mutations = new ArrayList<>();
      
      for (Entry entry : entries) {
        if (entry instanceof SetCell) {
            SetCell setCell = (SetCell) entry;
            mutations.add(Mutation.newBuilder()
                .setSetCell(Mutation.SetCell.newBuilder()
                    .setFamilyName(setCell.getFamilyName())
                    .setColumnQualifier(setCell.getQualifier())
                    .setTimestampMicros(setCell.getTimestamp())
                    .setValue(setCell.getValue())
                    .build())
                .build());
        } else if (entry instanceof DeleteCells) {
             DeleteCells deleteCells = (DeleteCells) entry;
             Range.TimestampRange range = deleteCells.getTimestampRange();
             mutations.add(Mutation.newBuilder()
                 .setDeleteFromColumn(Mutation.DeleteFromColumn.newBuilder()
                     .setFamilyName(deleteCells.getFamilyName())
                     .setColumnQualifier(deleteCells.getQualifier())
                     .setTimeRange(TimestampRange.newBuilder()
                         .setStartTimestampMicros(range.getStart())
                         .setEndTimestampMicros(range.getEnd())
                         .build())
                     .build())
                 .build());
        } else if (entry instanceof DeleteFamily) {
             DeleteFamily deleteFamily = (DeleteFamily) entry;
             mutations.add(Mutation.newBuilder()
                 .setDeleteFromFamily(Mutation.DeleteFromFamily.newBuilder()
                     .setFamilyName(deleteFamily.getFamilyName())
                     .build())
                 .build());
        }
      }
      return mutations;
    }
  }
}
