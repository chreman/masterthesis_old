# masterthesis

## Setup

* This README assumes you have an Amazon AWS account and a key pair for ssh/scp'ing to instances.

### Prepare S3 buckets

***Note*** Since all users on S3 share the same namespace, bucket names have to be unique. What worked for me is appending initials, for example.

* `data-ck`: going to hold raw data
* `logs-ck`: going to hold Spark logfiles
* `results-ck`: going to hold result files
* `workflows-ck`: going to hold pySpark-jobs and any additional resources like bootstrap scripts


### Preprocess raw data

Two scripts and one stylesheet do the work. For faster i/o this is best done on a small compute instance, I use Ubuntu 14.04 ami's with Anaconda 4. Please also look into setting up `boto3` and `awscli` first, this also needs to be done on the instance.

* `xml2json.py`: Contains the logic of opening a XML.gz, extracting the fulltext and metadata per article as defined by `JATS.json`, and convert it to JSON.
* `eupmc2s3.py`: Loads the fulltext corpus from EuropePMC via FTP, runs `xml2json.py` on each file, compresses the JSON to gz and uploads it to the data bucket on S3. Caching is implemented in a very basic way by simply noting down which files were processed successfully. Please be gentle and respectful when hitting other peoples server :)


1. Start an instance
1. `scp -i key.pem JATS.json ubuntu@public.dns.amazonaws.com:~/JATS.json`, also for `xml2json.py` and `eupmc2s3.py`
1. `ssh -i key.pem ubuntu@public.dns.amazonaws.com`
1. `python3 eupmc2s3.py`
1. check on S3 interface if data has arrived as expected

### Setting up a cluster

* Choose hardware and software
* Point to config files and bootstrap scripts
* Point to job and add arguments
* Some security configurations

### What is in workflows-ck?

* `workflow.py`: Contains the main analytical workflow for pySpark
* `bootstrap_python.sh`: What each computing node has to do at startup, e.g. install additional python libraries or a nltk-corpus file
* `gazetter.py`: A custom entity recognizer - still not working as intended
* `transformers.py`: Custom transformers for Spark, e.g. sentence tokenizer
* `spark-config.json`: Spark config file specified for this analysis
* `ConsecutiveNPTagger.pkl`: Prepared tagger so that it can be re-used - will be replaced at some point by the custom gazetteer

Add all those files in the workflows bucket on S3 - when setting up the cluster you will need to point at them at configuration stage.
