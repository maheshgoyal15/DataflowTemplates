
Cloud Bigtable Change Streams to Bigtable template
---
Streaming pipeline. Streams Bigtable data change records and writes them into
another Bigtable table using Dataflow Runner V2.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-bigtable-change-streams-to-bigtable)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Bigtable_Change_Streams_to_Bigtable).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **bigtableReadInstanceId**: The source Bigtable instance ID.
* **bigtableReadTableId**: The source Bigtable table ID.
* **bigtableChangeStreamAppProfile**: The Bigtable application profile ID. The application profile must use single-cluster routing and allow single-row transactions.
* **bigtableWriteInstanceId**: The ID of the Bigtable instance to write to.
* **bigtableWriteTableId**: The ID of the Bigtable table to write to.

### Optional parameters

* **bigtableWriteProjectId**: The ID of the Google Cloud project that contains the Bigtable instance to write to.
* **bigtableWriteAppProfile**: The ID of the Bigtable application profile to use for the export. If you do not specify an app profile, Bigtable uses the default app profile (https://cloud.google.com/bigtable/docs/app-profiles#default-app-profile) of the instance. Defaults to: default.
* **bigtableBulkWriteLatencyTargetMs**: The latency target of Bigtable in milliseconds for latency-based throttling.
* **bigtableBulkWriteMaxRowKeyCount**: The maximum number of row keys in a Bigtable batch write operation.
* **bigtableBulkWriteMaxRequestSizeBytes**: The maximum bytes to include per Bigtable batch write operation.
* **bigtableBulkWriteFlowControl**: When set to true, enables bulk write flow control which will use server's signal to throttle the writes. Defaults to: false.
* **bigtableReadProjectId**: The Bigtable project ID. The default is the project for the Dataflow job.
* **bigtableChangeStreamMetadataInstanceId**: The Bigtable change streams metadata instance ID. Defaults to empty.
* **bigtableChangeStreamMetadataTableTableId**: The ID of the Bigtable change streams connector metadata table. If not provided, a Bigtable change streams connector metadata table is automatically created during pipeline execution. Defaults to empty.
* **bigtableChangeStreamCharset**: The Bigtable change streams charset name. Defaults to: UTF-8.
* **bigtableChangeStreamStartTimestamp**: The starting timestamp (https://tools.ietf.org/html/rfc3339), inclusive, to use for reading change streams. For example, `2022-05-05T07:59:59Z`. Defaults to the timestamp of the pipeline start time.
* **bigtableChangeStreamIgnoreColumnFamilies**: A comma-separated list of column family name changes to ignore. Defaults to empty.
* **bigtableChangeStreamIgnoreColumns**: A comma-separated list of column name changes to ignore. Example: "cf1:col1,cf2:col2". Defaults to empty.
* **bigtableChangeStreamName**: A unique name for the client pipeline. Lets you resume processing from the point at which a previously running pipeline stopped. Defaults to an automatically generated name. See the Dataflow job logs for the value used.
* **bigtableChangeStreamResume**: When set to `true`, a new pipeline resumes processing from the point at which a previously running pipeline with the same `bigtableChangeStreamName` value stopped. If the pipeline with the given `bigtableChangeStreamName` value has never run, a new pipeline doesn't start. When set to `false`, a new pipeline starts. If a pipeline with the same `bigtableChangeStreamName` value has already run for the given source, a new pipeline doesn't start. Defaults to `false`.
* **bigtableReadChangeStreamTimeoutMs**: The timeout for Bigtable ReadChangeStream requests in milliseconds.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/bigtablechangestreamstobigtable/BigtableChangeStreamsToBigtable.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

#### Validating the Template

This template has a validation command that is used to check code quality.

```shell
mvn clean install -PtemplatesValidate \
-DskipTests -am \
-pl v2/googlecloud-to-googlecloud
```

### Building Template

This template is a Flex Template, meaning that the pipeline code will be
containerized and the container will be executed on Dataflow. Please
check [Use Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates)
and [Configure Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates)
for more information.

#### Staging the Template

If the plan is to just stage the template (i.e., make it available to use) by
the `gcloud` command or Dataflow "Create job from template" UI,
the `-PtemplatesStage` profile should be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export ARTIFACT_REGISTRY_REPO=<region>-docker.pkg.dev/$PROJECT/<repo>

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DartifactRegistry="$ARTIFACT_REGISTRY_REPO" \
-DstagePrefix="templates" \
-DtemplateName="Bigtable_Change_Streams_to_Bigtable" \
-pl v2/googlecloud-to-googlecloud -am
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Bigtable_Change_Streams_to_Bigtable
```

The specific path should be copied as it will be used in the following steps.

#### Running the Template

**Using the staged template**:

You can use the path above run the template (or share with others for execution).

To start a job with the template at any time using `gcloud`, you are going to
need valid resources for the required parameters.

Provided that, the following command line can be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Bigtable_Change_Streams_to_Bigtable"

### Required
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_WRITE_INSTANCE_ID=<bigtableWriteInstanceId>
export BIGTABLE_WRITE_TABLE_ID=<bigtableWriteTableId>

### Optional
export BIGTABLE_WRITE_PROJECT_ID=""
export BIGTABLE_WRITE_APP_PROFILE="default"
export BIGTABLE_READ_PROJECT_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET=UTF-8
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_CHANGE_STREAM_RESUME=false
export BIGTABLE_READ_CHANGE_STREAM_TIMEOUT_MS=<bigtableReadChangeStreamTimeoutMs>
export BIGTABLE_BULK_WRITE_LATENCY_TARGET_MS=<bigtableBulkWriteLatencyTargetMs>
export BIGTABLE_BULK_WRITE_MAX_ROW_KEY_COUNT=<bigtableBulkWriteMaxRowKeyCount>
export BIGTABLE_BULK_WRITE_MAX_REQUEST_SIZE_BYTES=<bigtableBulkWriteMaxRequestSizeBytes>
export BIGTABLE_BULK_WRITE_FLOW_CONTROL=false

gcloud dataflow flex-template run "bigtable-change-streams-to-bigtable-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID" \
  --parameters "bigtableReadTableId=$BIGTABLE_READ_TABLE_ID" \
  --parameters "bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE" \
  --parameters "bigtableWriteInstanceId=$BIGTABLE_WRITE_INSTANCE_ID" \
  --parameters "bigtableWriteTableId=$BIGTABLE_WRITE_TABLE_ID" \
  --parameters "bigtableWriteProjectId=$BIGTABLE_WRITE_PROJECT_ID" \
  --parameters "bigtableWriteAppProfile=$BIGTABLE_WRITE_APP_PROFILE" \
  --parameters "bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID" \
  --parameters "bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID" \
  --parameters "bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID" \
  --parameters "bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET" \
  --parameters "bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP" \
  --parameters "bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES" \
  --parameters "bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS" \
  --parameters "bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME" \
  --parameters "bigtableChangeStreamResume=$BIGTABLE_CHANGE_STREAM_RESUME" \
  --parameters "bigtableReadChangeStreamTimeoutMs=$BIGTABLE_READ_CHANGE_STREAM_TIMEOUT_MS" \
  --parameters "bigtableBulkWriteLatencyTargetMs=$BIGTABLE_BULK_WRITE_LATENCY_TARGET_MS" \
  --parameters "bigtableBulkWriteMaxRowKeyCount=$BIGTABLE_BULK_WRITE_MAX_ROW_KEY_COUNT" \
  --parameters "bigtableBulkWriteMaxRequestSizeBytes=$BIGTABLE_BULK_WRITE_MAX_REQUEST_SIZE_BYTES" \
  --parameters "bigtableBulkWriteFlowControl=$BIGTABLE_BULK_WRITE_FLOW_CONTROL"
```

For more information about the command, please check:
https://cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/run


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Required
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_WRITE_INSTANCE_ID=<bigtableWriteInstanceId>
export BIGTABLE_WRITE_TABLE_ID=<bigtableWriteTableId>

### Optional
export BIGTABLE_WRITE_PROJECT_ID=""
export BIGTABLE_WRITE_APP_PROFILE="default"
export BIGTABLE_READ_PROJECT_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET=UTF-8
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_CHANGE_STREAM_RESUME=false
export BIGTABLE_READ_CHANGE_STREAM_TIMEOUT_MS=<bigtableReadChangeStreamTimeoutMs>
export BIGTABLE_BULK_WRITE_LATENCY_TARGET_MS=<bigtableBulkWriteLatencyTargetMs>
export BIGTABLE_BULK_WRITE_MAX_ROW_KEY_COUNT=<bigtableBulkWriteMaxRowKeyCount>
export BIGTABLE_BULK_WRITE_MAX_REQUEST_SIZE_BYTES=<bigtableBulkWriteMaxRequestSizeBytes>
export BIGTABLE_BULK_WRITE_FLOW_CONTROL=false

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bigtable-change-streams-to-bigtable-job" \
-DtemplateName="Bigtable_Change_Streams_to_Bigtable" \
-Dparameters="bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID,bigtableReadTableId=$BIGTABLE_READ_TABLE_ID,bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE,bigtableWriteInstanceId=$BIGTABLE_WRITE_INSTANCE_ID,bigtableWriteTableId=$BIGTABLE_WRITE_TABLE_ID,bigtableWriteProjectId=$BIGTABLE_WRITE_PROJECT_ID,bigtableWriteAppProfile=$BIGTABLE_WRITE_APP_PROFILE,bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID,bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID,bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID,bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET,bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP,bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES,bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS,bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME,bigtableChangeStreamResume=$BIGTABLE_CHANGE_STREAM_RESUME,bigtableReadChangeStreamTimeoutMs=$BIGTABLE_READ_CHANGE_STREAM_TIMEOUT_MS,bigtableBulkWriteLatencyTargetMs=$BIGTABLE_BULK_WRITE_LATENCY_TARGET_MS,bigtableBulkWriteMaxRowKeyCount=$BIGTABLE_BULK_WRITE_MAX_ROW_KEY_COUNT,bigtableBulkWriteMaxRequestSizeBytes=$BIGTABLE_BULK_WRITE_MAX_REQUEST_SIZE_BYTES,bigtableBulkWriteFlowControl=$BIGTABLE_BULK_WRITE_FLOW_CONTROL" \
-f v2/googlecloud-to-googlecloud
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v2/googlecloud-to-googlecloud/terraform/Bigtable_Change_Streams_to_Bigtable
terraform init
terraform apply
```

To use
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly:

```terraform
provider "google-beta" {
  project = var.project
}
variable "project" {
  default = "<my-project>"
}
variable "region" {
  default = "us-central1"
}

resource "google_dataflow_flex_template_job" "bigtable_change_streams_to_bigtable" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Bigtable_Change_Streams_to_Bigtable"
  name              = "bigtable-change-streams-to-bigtable"
  region            = var.region
  parameters        = {
    bigtableReadInstanceId = "<bigtableReadInstanceId>"
    bigtableReadTableId = "<bigtableReadTableId>"
    bigtableChangeStreamAppProfile = "<bigtableChangeStreamAppProfile>"
    bigtableWriteInstanceId = "<bigtableWriteInstanceId>"
    bigtableWriteTableId = "<bigtableWriteTableId>"
    # bigtableWriteProjectId = ""
    # bigtableWriteAppProfile = "default"
    # bigtableReadProjectId = ""
    # bigtableChangeStreamMetadataInstanceId = ""
    # bigtableChangeStreamMetadataTableTableId = ""
    # bigtableChangeStreamCharset = "UTF-8"
    # bigtableChangeStreamStartTimestamp = ""
    # bigtableChangeStreamIgnoreColumnFamilies = ""
    # bigtableChangeStreamIgnoreColumns = ""
    # bigtableChangeStreamName = "<bigtableChangeStreamName>"
    # bigtableChangeStreamResume = "false"
    # bigtableReadChangeStreamTimeoutMs = "<bigtableReadChangeStreamTimeoutMs>"
    # bigtableBulkWriteLatencyTargetMs = "<bigtableBulkWriteLatencyTargetMs>"
    # bigtableBulkWriteMaxRowKeyCount = "<bigtableBulkWriteMaxRowKeyCount>"
    # bigtableBulkWriteMaxRequestSizeBytes = "<bigtableBulkWriteMaxRequestSizeBytes>"
    # bigtableBulkWriteFlowControl = "false"
  }
}
```
