"""
Biotic
Interaction
Network
Extractor

Master thesis project by Christopher Kittel
c.kittel [at] uni-graz.at
christopherkittel.eu
"""

from os import path
import pickle
import json, csv
import argparse
from itertools import chain
import logging
from collections import OrderedDict

from pyspark import SparkContext, SQLContext, SparkConf
from pyspark import SparkFiles

from pyspark.sql.functions import dayofmonth, month, year, to_date
from pyspark.sql.functions import col, sum, explode, udf, concat, lit
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, IntegerType, BooleanType, MapType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, RegexTokenizer, StopWordsRemover, CountVectorizer, VectorSlicer, NGram, VectorAssembler

import pandas as pd
import nltk

from gazetteer import ConsecutiveNPChunker
from transformers import StringListAssembler, ColumnExploder, ColumnSelector, SentTokenizer



def main(args):
    FORMAT = '%(asctime)-15s %(message)s'
    if args.logfile:
        logging.basicConfig(format=FORMAT, level=logging.INFO, filename=args.logfile)
    else:
        logging.basicConfig(format=FORMAT, level=logging.INFO)
    logger = logging.getLogger('sparklogger')
    logger.info('Beginning workflow')
    conf = SparkConf()
    if (args.awsAccessKeyID and args.awsSecretAccessKey):
        conf.set("spark.hadoop.fs.s3.awsAccessKeyID", args.awsAccessKeyID)
        conf.set("spark.hadoop.fs.s3.awsSecretAccessKey", args.awsSecretAccessKey)
    sc = SparkContext(conf=conf)

    sc.addFile(args.tagger)
    sc.addFile(args.entities)

    sqlContext = SQLContext(sc)
    RDD_jsonstrings = sc.textFile(args.input)
    RDD_dicts = RDD_jsonstrings.map(json.loads)
    logger.info('Loading RDDs from %s.' %args.input)
    df = sqlContext.createDataFrame(RDD_dicts, samplingRatio=0.01)
    df = df.dropDuplicates(['doi'])
    if args.sample:
        df = df.sample(False, float(args.sample), 42)
    df.cache()
    logger.info('Base df created, papers in sample: %d' %df.count())

    ########################
    # Adding additional files

    with open(SparkFiles.get("entities.csv"), "r") as infile:
        reader = csv.reader(infile, delimiter=';', quotechar='"')
        rows = reader
        entities = {}
        for row in rows:
            entity = row[0]
            aliases = row[1].split(",")
            if len(entity.split(" ")) == 2:
                entities[entity] = aliases
    all_entities = [a.lower() for a in list(chain.from_iterable(list(entities.values())))]
    logger.info('Entity terms loaded to driver: %d' %len(all_entities))

    #########################
    # FEATURE ENGINEERING
    logger.info('Initialising transformers and pipeline.')
    sentTokenizer = SentTokenizer(inputCol="fulltext", outputCol="sentence_list")
    selector = ColumnSelector(outputCols=["doi", "sentence_list"])
    cexploder = ColumnExploder(inputCol="sentence_list", outputCol="sentences")
    tokenizer = Tokenizer(inputCol="sentences", outputCol="words")
    swremover = StopWordsRemover(inputCol="words", outputCol="filtered")
    bigramer = NGram(inputCol="filtered", outputCol="bigrams", n=2)
    sassembler = StringListAssembler(inputCols=["filtered", "bigrams"], outputCol="raw_features")
    cv = CountVectorizer(inputCol="raw_features", outputCol="features")

    #########################
    # PIPELINE

    pipeline = Pipeline(stages=[sentTokenizer, selector, cexploder, tokenizer, swremover, bigramer, sassembler, cv])
    pipeline_model = pipeline.fit(df)
    logger.info('Fitting pipeline model.')
    result_df = pipeline_model.transform(df)
    logger.info('Applying pipeline model.')
    result_df.cache()

    #########################
    # SETUP BROADCAST VARIABLES

    vocabulary = pipeline_model.stages[-1].vocabulary
    logger.info('Size of vocabulary: %d' %len(vocabulary))
    indices = []
    index2entity = {}
    is_in = set(vocabulary).intersection(set(all_entities))
    logger.info('Unique entity matches in dataset: %d' %len(is_in))

    for e in is_in:
        index = vocabulary.index(e)
        indices.append(index)
        index2entity[index] = e

    I = sc.broadcast(indices)
    I2E = sc.broadcast(index2entity)

    logger.info('Broadcast variables broadcasted.')

    ##########################
    # UDF REGISTRATIONS

    def get_boolean_occcurrence(sv):
        if len(set(I.value).intersection(set(sv.indices))) > 0:
            return True
        else:
            return False
    logger.info('Registering udf_boolean_occurrence.')
    udf_boolean_occurrence = udf(get_boolean_occcurrence, BooleanType())

    def get_length(x):
        return len(x)
    logger.info('Registering udf_length.')
    udf_length = udf(get_length, IntegerType())

    def map_sv2entities(sv):
        from collections import Counter
        mapper = I2E.value
        alias_counts = {}
        for k, v in list(zip(sv.indices, sv.values)):
            if k in mapper:
                a = mapper[k]
                alias_counts[a] = int(v)
        return alias_counts
    count_schema = MapType(StringType(), IntegerType())
    logger.info('Registering udf_map_sv2entities.')
    udf_map_sv2entities = udf(map_sv2entities, count_schema)

    def total_count(entity_counts):
        counts = 0
        for gc in entity_counts:
            counts += 1
        return counts
    logger.info('Registering udf_total_count.')
    udf_total_count = udf(total_count, IntegerType())


    train_sents = nltk.corpus.conll2000.chunked_sents('train.txt', chunk_types=['NP'])
    chunker = ConsecutiveNPChunker(train_sents)
    C = sc.broadcast(chunker)

    def get_triples(sentence):
        import nltk
        import re
        tagged_sentence = nltk.pos_tag(nltk.wordpunct_tokenize(sentence))
        tree = C.value.parse(tagged_sentence)
        semirels = nltk.sem.relextract.tree2semi_rel(tree)
        relations = nltk.sem.relextract.semi_rel2reldict(semirels)
        # pattern = re.compile('ate|eats|feeds|preys')
        pattern = re.compile(r'\/VBD$|\/VBD.*\/VBN|\/RB.*\/VB')
        triples = []
        for rel in relations:
            if pattern.search(rel.get('filler')):
                new_triple = (rel.get("subjtext"), rel.get("filler"), rel.get("objtext"))
                triples.append(new_triple)
        return triples
    logger.info('Registering udf_get_triples.')
    udf_get_triples = udf(get_triples, ArrayType(ArrayType(StringType())))

    logger.info('UDFs registered.')

    ##########################
    # PREPARE OUTPUT

    result_df = result_df.select(['doi', 'sentences', 'features']).withColumn('hits', udf_boolean_occurrence(result_df['features']))
    # output = result_df.select(['doi', 'title', 'features']).withColumn('hits', udf_boolean_occurrence(result_df['features']))
    output = result_df.filter(result_df['hits'] == True)
    logger.info('Output filtered, number of entries with matches: %d' %output.count())
    output = output.withColumn('entity_matches', udf_map_sv2entities(output['features']))
    output = output.withColumn('entity_counts', udf_total_count(output['entity_matches']))
    output = output.withColumn('triples', udf_get_triples(output['sentences']))
    output = output.withColumn('triples_counts', udf_total_count(output['triples']))
    output.cache()

    ##########################
    # WRITE OUTPUT
    output = output.select('doi', 'sentences', 'entity_matches', 'entity_counts', 'triples','triples_counts') \
                .filter(output['entity_counts'] >= int(args.entity_counts)) \
                .filter(output['triples_counts'] >= int(args.triples_counts)) \
                .orderBy(['triples_counts'], ascending=[0])
    output.write.json(args.output)

    logger.info('Final results filtered, number of results: %d' %output.count())
    logger.info('Ending workflow, shutting down.')
    sc.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='do stuff')
    parser.add_argument('--input', dest='input', help='relative or absolute path of the input folder')
    parser.add_argument('--output', dest='output', help='relative or absolute path of the output folder')
    parser.add_argument('--entity-counts', dest='entity_counts', help='min_number of entity counts')
    parser.add_argument('--triples-counts', dest='triples_counts', help='min_number of triples counts')
    parser.add_argument('--entities', dest='entities', help='relative or absolute path of the entities file')
    parser.add_argument('--tagger', dest='tagger', help='relative or absolute path of the tagger file')
    parser.add_argument('--logfile', dest='logfile', help='relative or absolute path of the logfile')
    parser.add_argument('--sample', dest='sample', help='fraction of data to use as sample')
    parser.add_argument('--awsAccessKeyID', dest='awsAccessKeyID', help='awsAccessKeyID')
    parser.add_argument('--awsSecretAccessKey', dest='awsSecretAccessKey', help='awsSecretAccessKey')
    args = parser.parse_args()
    main(args)
