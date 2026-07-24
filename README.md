## virtualizarr-data-pipelines

Virtualizarr Data Pipelines is a [github template repository](https://docs.github.com/en/repositories/creating-and-managing-repositories/creating-a-repository-from-a-template) intended to help users create and manage Virtualizarr/Icechunk stores on AWS in a consistent, scalable way.

The goal is to let users leverage their expertise to focus on how to parse and
concatenate archival files without having to think too much about
building infrastructure code.

### Backfill vs Forward

Virtualizarr Data Pipelines supports two complementary paths for getting files virtualized and into an Icechunk store:

- **Backfill processing** is a one time, high-throughput bulk load of a large body of
  *existing* files. It initializes
  the Icechunk store with full shape (for example, every time step covered by the existing files) and uses a partitioned Icechunk **fork and merge**
  [cooperative distributed write] (https://icechunk.io/en/stable/understanding/parallel/#cooperative-distributed-writes) approach so many thousands of files can be processed in parallel with a small number of commits. It uses AWS Step Functions to orchestrate this work and is disabled by default
  (enable it with the `BACKFILL_ENABLED` environment setting).
- **Forward queue processing** is the path for processing *new production files as they
  become available*. Files are announced to an SQS queue (typically via S3/SNS
  notifications), consumed by a Lambda, appended to the `main` branch, and committed
  per batch.

A typical project uses both: run **backfill** once to load the historical archive, then
rely on **forward queue** processing to keep the store current as new files land.

### Getting started :rocket:
First [create your own repository from the
template](https://docs.github.com/en/repositories/creating-and-managing-repositories/creating-a-repository-from-a-template).
You'll use this repository to build and configure your own dataset specific
pipeline. We recommend the naming convention `datasetname-virtualizarr-data-pipelines`.

### Creating a processor :package:
Once you have your own repo, the first step is building your own processor module. There is a sample
[processor.py](./lambda/virtualizarr-processor/virtualizarr_processor/processor.py) in the repo that uses an in-memory Icechunk store and a fake virtual dataset to
demonstrate how a processor works.  Replace this with your own `processor.py`
file.  Your class should follow the [VirtualizarrProcessor protocol](./lambda/virtualizarr-processor/virtualizarr_processor/typing.py).

You can specify the dependencies for your processor module in its [pyproject.toml](./lambda/virtualizarr-processor/pyproject.toml).

You should create tests for your module in the [tests](./tests) directory. There are sample fixtures for an in memory Icechunk store and some basic sample tests for the sample processor module in the template repo that you can use as a guide.

The Virtualizarr Data Pipelines CDK infrastructure will use this module to create Docker images, Lambda functions and an AWS Batch job for initializing the Icechunk store, consuming SQS messages for files and appending them to the store and running Icechunk garbage collection. When backfill is enabled it also builds the backfill Step Functions orchestration described below.

### Configuring the deployment :wrench:
Virtualizarr Data Pipelines uses a strongly-typed [settings module](./cdk/settings.py) that allows you to configure things like bucket names and external SNS topics used by the CDK infrastructure when you deploy it.  Many of the settings include defaults but you can also specify and override values with a `.env` file.  A [sample file](./.env.sample) is provided as an example.

Here is where you can specify things like the SNS topic you created to feed your queue.  Or the S3 bucket where your archival dataset lives.

#### Forward queue processing :cookie:
Forward processing handles **new production files as they become available**.
Virtualizarr Data Pipelines is only responsible for creating a store and processing file
notifications fed to its SQS queue.  You'll be responsible for getting messages in this queue.
Each message is a file to parse and append to the `main` branch, and the queue consumer
commits once per batch of files (the number of file messages sent to a single
Lambda invocation is controlled by `SQS_BATCH_SIZE`.

For S3 buckets where new data is continually added you can enable an [SNS topic for new data](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ways-to-add-notification-config-to-bucket.html) which the Virtualizarr Data Pipelines queue can subscribe to, so files are processed as they land.  This can be configured using `SNS_TOPIC` which will automatically wire up notifications to the queue.

![Architecture](./docs/architecture-dark.png#gh-dark-mode-only)
![Architecture](./docs/architecture.png#gh-light-mode-only)

The `processor` protocol methods below drive **forward processing**:

- **initialize_repo** This method should create your new Icechunk store and use a
  seed file to initialize the structure that you can append subsequent files to.

- **initialize_session** This method takes the repository from above and returns
  a writable Icechunk session.

- **process_file** This method should take a file uri and a session and use a Virtualizarr parser to parse it and add the resulting ManifestStore or virtual dataset to the Icechunk store.

- **commit_processed_files** This method commits all the changes made during the
  session in a single commit.

- **garbage_collect** This method runs Icechunk garbage collection and snapshot
  removal for snapshots older than a given expiry time. It is shared by both
  processing modes and is invoked on the schedule set by `GARBAGE_COLLECTION_FREQUENCY`.


### Backfill processing

Backfill processes a large set of existing files in a single, highly
parallel run. Instead of appending each file to `main` — where many concurrent workers
would contend for the branch tip — it declares the store at its **full shape** up front on
a dedicated `backfill` branch and then uses Icechunk's **fork and merge** model.  

1. A coordinator takes an inventory and splits it into partitions.  Each
   partition will be processed serially and will be written to the Icechunk
   store as a single commit.  You'll want to balance your partitioning size so you're
   making a reasonably small number of commits but not losing too much work if
   one of the jobs in your partition fails (which means all the files in that
   partition will not be committed). 
2. The coordinator creates an Icechunk store with the dataset's full dimension extent.
3. For the first partition, the coordinator forks a clean, committed base snapshot.  
4. Each partition step spawns a number of workers.  Each worker copies the fork and writes a **disjoint** region of the array via
`vds.vz.to_icechunk(fork.store, region="auto")` without committing.
Region distjointness is the operator's responsibility, trying to write to the same region will result in merge failures.
5. A reducer function merges all the child worker forks into **one commit for the partition** and finally `main` is fast-forwarded to
the backfill tip. Because every worker writes to an independent fork and only the reducer
commits, there is no tip contention and the writes-per-commit ratio is maximized.
6. Each partition is processed serially so after the first partition is
   committed a new fork is created and used by the next partition.

The pipeline is orchestrated by AWS Step Functions: an outer serial Map over partitions,
each running Fork → an inner Distributed Map of parallel worker Lambdas → Reduce, followed by a final Promote.

![Backfill](./docs/backfill-fork-merge-dark.png#gh-dark-mode-only)
![Backfill](./docs/backfill-fork-merge.png#gh-light-mode-only)

#### Backfill processor methods

For backfill your processor needs to implement these additional [VirtualizarrProcessor protocol](./lambda/virtualizarr-processor/virtualizarr_processor/typing.py)
methods (in addition to the forward queue methods above):

- initialize_backfill_store Set up the empty Icechunk store workers will fill. Runs once at Init: it creates a backfill branch off the current main tip (staging the load to the side until the final promote), creates the array(s) at their full final shape with coordinate arrays (e.g. time) written as metadata, and commits. It must commit and leave nothing pending, since workers fork from this snapshot and a fork can only be merged if its base is a clean committed snapshot.

- open_backfill_repo Open the Icechunk repository and return a Repository handle. Every backfill step that touches the store (Init, Fork, Reduce, Promote) calls this to get the same `repo`.

- **process_backfill_file** Write a single file's virtual dataset into the worker's fork at via `vz.to_icechunk(store, region="auto")`. It must **not** commit.

#### Backfill configuration

Backfill is configured through the same [settings module](./cdk/settings.py) / `.env` file
as the rest of the deployment. Settings specific to backfill:

- **BACKFILL_ENABLED** (default `false`) — deploy the backfill Step Functions pipeline.
  Leave this off if you only need forward queue processing.
- **BACKFILL_PARTITION_SIZE** (default `500`) — number of files per partition. Each
  partition becomes one merged commit.
- **BACKFILL_MAX_ITEMS_PER_BATCH** (default `10`) — number of file keys processed by each worker Lambda (the inner Distributed Map's batch size). Each batch becomes one child fork.
- **BACKFILL_MAX_CONCURRENCY** (default `50`) — maximum number of worker Lambdas running in parallel within a partition.  Note that if you are using dependent rate limited APIs like NASA EDL use appropriate settings here.

Backfill also relies on `ICECHUNK_BUCKET` (the S3 bucket holding the Icechunk store and the
per-run fork artifacts) and `DATA_BUCKET_NAME` (the source bucket workers read files from).


### Project commands :hammer:
#### To set up the development environment
```
./scripts/setup.sh
```

#### Run tests
```
uv run pytest
```

#### Review your infrastructure before deploying
```
uv run --env-file .env.sample cdk synth

```
#### Deploy the CDK infrastructure.
```

uv run --env-file .env.sample cdk deploy
```
