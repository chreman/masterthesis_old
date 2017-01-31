#!/bin/bash -xe
sudo pip install -U \
spark \
pandas \
py4j \
nltk
python -m nltk.downloader punkt
python -m nltk.downloader tagsets
python -m nltk.downloader averaged_perceptron_tagger
python -m nltk.downloader hmm_treebank_pos_tagger
python -m nltk.downloader maxent_ne_chunker
python -m nltk.downloader maxent_treebank_pos_tagger
python -m nltk.downloader stopwords
python -m nltk.downloader words
python -m nltk.downloader conll2000
