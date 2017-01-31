# masterthesis

## Setup

* This README assumes you have an Amazon AWS account and a key pair for ssh/scp'ing to instances.

### Prepare S3 buckets

*Note* Since all users on S3 share the same namespace, bucket names have to be unique. What worked for me is appending initials, for example.

* `data-ck`: going to hold raw data
* `logs-ck`: going to hold Spark logfiles
* `results-ck`: going to hold result files
* `workflows-ck`: going to hold pySpark-jobs and any additional resources like bootstrap scripts


### Preprocess raw data



### Setting up a cluster

* Choose hardware and software
* Point to config files and bootstrap scripts
* Point to job and add arguments
* Some security configurations
