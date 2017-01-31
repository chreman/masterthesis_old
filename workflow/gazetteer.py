"""
Gazetter based on
http://stackoverflow.com/questions/30150047/find-all-locations-cities-places-in-a-text

ChunkTagger based on
http://www.nltk.org/book/ch07.html
"""

import re

import nltk
from nltk.chunk.util import conlltags2tree
from nltk.chunk import ChunkParserI


class Gazetteer(ChunkParserI):
    """
    Find and annotate a list of words that matches patterns.
    Patterns may be regular expressions in the form list of tuples.
    Every tuple has the regular expression and the iob tag for this one.
    Before applying gazetteer words a part of speech tagging should
    be performed. So, you have to pass your tagger as a parameter.
    Example:
        >>> patterns = [(u"Αθήνα[ς]?", "LOC"), (u"Νομική[ς]? [Σσ]χολή[ς]?", "ORG")]
        >>> gazetteer = Gazetteer(patterns, nltk.pos_tag, nltk.wordpunct_tokenize)
        >>> text = u"Η Νομική σχολή της Αθήνας"
        >>> t = gazetteer.parse(text)
        >>> print(unicode(t))
        ... (S Η/DT (ORG Νομική/NN σχολή/NN) της/DT (LOC Αθήνας/NN))
    """

    def __init__(self, patterns, pos_tagger, tokenizer):
        """
        Initialize the class.

        :param patterns:
            The patterns to search in text is a list of tuples with regular
            expression and the tag to apply
        :param pos_tagger:
            The tagger to use for applying part of speech to the text
        :param tokenizer:
            The tokenizer to use for tokenizing the text
        """
        self.patterns = patterns
        self.pos_tag = pos_tagger
        self.tokenize = tokenizer
        self.lookahead = 0  # how many words it is possible to be a gazetteer word
        self.words = []  # Keep the words found by applying the regular expressions
        self.iobtags = []  # For each set of words keep the coresponding tag
        self.progs = [(re.compile(pattern), tag) for pattern, tag in self.patterns]

    def iob_tags(self, tagged_sent):
        """
        Search the tagged sentences for gazetteer words and apply their iob tags.

        :param tagged_sent:
            A tokenized text with part of speech tags
        :type tagged_sent: list
        :return:
            yields the IOB tag of the word with it's character, eg. B-LOCATION
        :rtype:
        """
        i = 0
        l = len(tagged_sent)
        inside = False  # marks the I- tag
        iobs = []

        while i < l:
            word, pos_tag = tagged_sent[i]
            j = i + 1  # the next word
            k = j + self.lookahead  # how many words in a row we may search
            nextwords, nexttags = [], []  # for now, just the ith word
            add_tag = False  # no tag, this is O

            while j <= k:
                words = ' '.join([word] + nextwords)  # expand our word list
                if words in self.words:  # search for words
                    index = self.words.index(words)  # keep index to use for iob tags
                    if inside:
                        iobs.append((word, pos_tag, 'I-' + self.iobtags[index]))  # use the index tag
                    else:
                        iobs.append((word, pos_tag, 'B-' + self.iobtags[index]))

                    for nword, ntag in zip(nextwords, nexttags):  # there was more than one word
                        iobs.append((nword, ntag, 'I-' + self.iobtags[index]))  # apply I- tag to all of them

                    add_tag, inside = True, True
                    i = j  # skip tagged words
                    break

                if j < l:  # we haven't reach the length of tagged sentences
                    nextword, nexttag = tagged_sent[j]  # get next word and it's tag
                    nextwords.append(nextword)
                    nexttags.append(nexttag)
                    j += 1
                else:
                    break

            if not add_tag:  # unkown words
                inside = False
                i += 1
                iobs.append((word, pos_tag, 'O'))  # it's an Outsider

        return iobs

    def parse(self, text, conlltags=True):
        """
        Given a text, applies tokenization, part of speech tagging and the
        gazetteer words with their tags. Returns an conll tree.

        :param text: The text to parse
        :type text: str
        :param conlltags:
        :type conlltags:
        :return: An conll tree
        :rtype:
        """
        # apply the regular expressions and find all the
        # gazetteer words in text
        for prog, tag in self.progs:
            words_found = set(prog.findall(text))  # keep the unique words
            if len(words_found) > 0:
                for word in words_found:  # words_found may be more than one
                    self.words.append(word)  # keep the words
                    self.iobtags.append(tag)  # and their tag

        # find the pattern with the maximum words.
        # this will be the look ahead variable
        for word in self.words:  # don't care about tags now
            nwords = word.count(' ')
            if nwords > self.lookahead:
                self.lookahead = nwords

        # tokenize and apply part of speech tagging
        tagged_sent = self.pos_tag(self.tokenize(text))
        # find the iob tags
        iobs = self.iob_tags(tagged_sent)

        if conlltags:
            return conlltags2tree(iobs)
        else:
            return iobs


class ConsecutiveNPChunker(nltk.ChunkParserI):
    def __init__(self, train_sents):
        tagged_sents = [[((w,t),c) for (w,t,c) in
                         nltk.chunk.tree2conlltags(sent)]
                        for sent in train_sents]
        self.tagger = ConsecutiveNPChunkTagger(tagged_sents)

    def parse(self, sentence):
        tagged_sents = self.tagger.tag(sentence)
        conlltags = [(w,t,c) for ((w,t),c) in tagged_sents]
        return nltk.chunk.conlltags2tree(conlltags)

class ConsecutiveNPChunkTagger(nltk.TaggerI):

    def __init__(self, train_sents):
        train_set = []
        for tagged_sent in train_sents:
            untagged_sent = nltk.tag.untag(tagged_sent)
            history = []
            for i, (word, tag) in enumerate(tagged_sent):
                featureset = self.npchunk_features(untagged_sent, i, history)
                train_set.append( (featureset, tag) )
                history.append(tag)
        self.classifier = nltk.NaiveBayesClassifier.train(train_set)

    def tag(self, sentence):
        history = []
        for i, word in enumerate(sentence):
            featureset = self.npchunk_features(sentence, i, history)
            tag = self.classifier.classify(featureset)
            history.append(tag)
        return zip(sentence, history)

    def npchunk_features(self, sentence, i, history):
        word, pos = sentence[i]
        if i == 0:
            prevword, prevpos = "<START>", "<START>"
        else:
            prevword, prevpos = sentence[i-1]
        if i == len(sentence)-1:
            nextword, nextpos = "<END>", "<END>"
        else:
            nextword, nextpos = sentence[i+1]
        return {"pos": pos,
                "word": word,
                "prevpos": prevpos,
                "nextpos": nextpos,
                "prevpos+pos": "%s+%s" % (prevpos, pos),
                "pos+nextpos": "%s+%s" % (pos, nextpos),
                "tags-since-dt": self.tags_since_dt(sentence, i)}

    def tags_since_dt(self, sentence, i):
        tags = set()
        for word, pos in sentence[:i]:
            if pos == 'DT':
                tags = set()
            else:
                tags.add(pos)
        return '+'.join(sorted(tags))
