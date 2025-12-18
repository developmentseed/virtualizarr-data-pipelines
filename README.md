## virtualizarr-data-pipelines

Virtualizarr Data Pipelines is a [github template
repository](https://docs.github.com/en/repositories/creating-and-managing-repositories/creating-a-repository-from-a-template) intended to help users create and manage Virtualizarr/Icechunk stores on AWS in a consistent, scalable way.

The goal is to let users leverage their expertise to focus on how to parse and
concatenate archival files without having to think too much about
infrastructure code.

### Getting started :rocket:
First [create your own repository from the
template](https://docs.github.com/en/repositories/creating-and-managing-repositories/creating-a-repository-from-a-template). You'll use this repository to build and configure your own dataset specific pipeline.

#### Creating a processor :package:
Once you have your own repo, the first step is building your own processor module. There is a sample
[processor.py](./lambda/virtualizarr-processor/virtualizarr_processor/processor.py) in the repo that uses an in-memory Icechunk store and a fake virtual dataset to
demonstrate how a processor works.  Replace this with your own `processor.py`
file.  Your class should follow the [VirtualizarrProcessor protocol](./lambda/virtualizarr-processor/virtualizarr_processor/typing.py).

- **initialize_store** This method should create your new Icechunk store and use a
  seed file to initialize the structure that you can append subsequent files to.

- **process_file** This method should take a file uri, use a Virtualizarr parser to
    parse it and add the resulting ManifestStore or virtual dataset to the
    Icechunk store.

You can specify the dependencies for your processor module in its [pyproject.toml](./lambda/virtualizarr-processor/pyproject.toml).

You should create tests for your module in the [tests](./tests) directory. There are sample fixtures for an in memory Icechunk store and some basic sample tests for the sample processor module in the template repo that you can use as a guide.

The Virtualizarr Data Pipelines CDK infrastructure will use this module to create a Docker images, Lambda functions and an AWS Batch job for initializing the Icechunk store, consuming SQS messages for files and appending them to the store and running Icechunk garbage collection.

### Feeding the queue :cookie:
Virtualizarr Data Pipelines is only responsible for creating a store and processing file notifications fed to its queue.  You'll be responsible for getting messages in this queue.  The queue is not just for newly produced data but provides a common approach for processing large numbers of existing files for one off efforts for archival data.

For existing archival data in S3 the simplest approach is enabling S3 inventories on the bucket and using [Athena to query the inventories](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory-athena-query.html) and push messages onto the queue in batches of a manageable size.

For S3 buckets where new data is continually added you can enable an [SNS topic for new data](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ways-to-add-notification-config-to-bucket.html) which the Virtualizarr Data Pipelines queue can subscribe to.
[![Architecture](./docs/architecture.png)](./docs/architecture.png)

### Configuring the deployment :wrench:
Virtualizarr Data Pipelines uses a strongly-typed [settings module](./cdk/settings.py) that allows you to configure things like bucket names and external SNS topics used by the CDK infrastructure when you deploy it.  Many of the settings include defaults but you can also specify and override values with a `.env` file.  A [sample file](./.env.sample) is provided as an example.

Here is where you can specify things like the SNS topic you created to feed your
queue.  Or the S3 bucket where your archival dataset lives.

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
