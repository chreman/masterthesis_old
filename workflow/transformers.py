from pyspark import keyword_only  ## < 2.0 -> pyspark.ml.util.keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, StringType, IntegerType

class NLTKWordPunctTokenizer(Transformer, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, stopwords=None):
        super(NLTKWordPunctTokenizer, self).__init__()
        self.stopwords = Param(self, "stopwords", "")
        self._setDefault(stopwords=set())
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, stopwords=None):
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setStopwords(self, value):
        self._paramMap[self.stopwords] = value
        return self

    def getStopwords(self):
        return self.getOrDefault(self.stopwords)

    def _transform(self, df):
        stopwords = self.getStopwords()

        def f(s):
            import nltk
            nltk.data.path.append("~/nltk_data")
            nltk.data.path.append("/home/hadoop/nltk_data")
            tokens = nltk.tokenize.wordpunct_tokenize(s)
            return [t for t in tokens if t.lower() not in stopwords]

        t = ArrayType(StringType())
        out_col = self.getOutputCol()
        in_col = df[self.getInputCol()]
        return df.withColumn(out_col, udf(f, t)(in_col))


class StringListAssembler(Transformer, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, inputCols=None, outputCol=None):
        super(StringListAssembler, self).__init__()
        # kwargs = self.__init__._input_kwargs
        # self.setParams(**kwargs)
        self.inputCols = inputCols
        self.outputCol = outputCol

    @keyword_only
    def setParams(self, inputCols=None, outputCol=None):
        # kwargs = self.setParams._input_kwargs
        # return self._set(**kwargs)
        pass

    def _transform(self, df):

        def concatenate_lists(*cols):
            cl = []
            for col in cols:
                cl.extend(col)
            return cl
        schema = ArrayType(StringType())
        udf_concatenate_lists = udf(concatenate_lists, schema)
        if len(self.inputCols) == 2:
            col1, col2 = self.inputCols
            return df.withColumn(self.outputCol, udf_concatenate_lists(df[col1], df[col2]))
        if len(self.inputCols) == 3:
            col1, col2, col3 = self.inputCols
            return df.withColumn(self.outputCol, udf_concatenate_lists(df[col1], df[col2], df[col3]))


class SentTokenizer(Transformer, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(SentTokenizer, self).__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol

    @keyword_only
    def setParams(self, inputCols=None, outputCol=None):
        pass

    def _transform(self, df):
        def sent_tokenize(x):
            import nltk
            nltk.data.path.append("~/nltk_data")
            nltk.data.path.append("/home/hadoop/nltk_data")
            return nltk.sent_tokenize(x)

        udf_sent_tokenize = udf(sent_tokenize, ArrayType(StringType()))
        return df.withColumn(self.outputCol, udf_sent_tokenize(df[self.inputCol]))

class TripleExtractor(Transformer, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, patterns=None):
        super(SentTokenizer, self).__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol
        self.patterns = patterns

    @keyword_only
    def setParams(self, inputCols=None, outputCol=None):
        pass

    def _transform(self, df):
        def get_relations(sentence):
            import nltk
            nltk.data.path.append("~/nltk_data")
            nltk.data.path.append("/home/hadoop/nltk_data")
            import re
            import itertools
            sentence = nltk.word_tokenize(sentence)
            sentence = nltk.pos_tag(sentence)
            tree = nltk.ne_chunk(sentence)
            semirels = nltk.sem.relextract.tree2semi_rel(tree)
            relations = nltk.sem.relextract.semi_rel2reldict(semirels)
            triples = []
            for rel in relations:
                new_triple = (rel.get("subjtext"), rel.get("filler"), rel.get("objtext"))
                triples.append(new_triple)
            return triples
        def get_custom_relations(sentence, patterns):
            import gazetteer
            import nltk
            nltk.data.path.append("~/nltk_data")
            nltk.data.path.append("/home/hadoop/nltk_data")
            import re
            g = gazetteer.Gazetteer(patterns, nltk.pos_tag, nltk.wordpunct_tokenize)
            tree = g.parse(sentence)
            semirels = nltk.sem.relextract.tree2semi_rel(tree)
            relations = nltk.sem.relextract.semi_rel2reldict(semirels)
            triples = []
            for rel in relations:
                new_triple = (rel.get("subjtext"), rel.get("filler"), rel.get("objtext"))
                triples.append(new_triple)
            return triples

        if not self.patterns:
            udf_get_relations = udf(get_relations, ArrayType(ArrayType(StringType())))
            return df.withColumn(self.outputCol, udf_get_relations(df[self.inputCol]))
        else:
            udf_get_relations = udf(get_custom_relations, ArrayType(ArrayType(StringType())))
            return df.withColumn(self.outputCol, udf_get_relations(df[self.inputCol], self.patterns))

class ColumnExploder(Transformer, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(ColumnExploder, self).__init__()
        # kwargs = self.__init__._input_kwargs
        # self.setParams(**kwargs)
        self.inputCol = inputCol
        self.outputCol = outputCol

    @keyword_only
    def setParams(self, inputCols=None, outputCol=None):
        # kwargs = self.setParams._input_kwargs
        # return self._set(**kwargs)
        pass

    def _transform(self, df):
        df = df.select('*', explode(df[self.inputCol]).alias(self.outputCol))
        return df


class ColumnSelector(Transformer, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, outputCols=None):
        super(ColumnSelector, self).__init__()
        # kwargs = self.__init__._input_kwargs
        # self.setParams(**kwargs)
        self.outputCols = outputCols

    @keyword_only
    def setParams(self, inputCols=None, outputCol=None):
        # kwargs = self.setParams._input_kwargs
        # return self._set(**kwargs)
        pass

    def _transform(self, df):
        df = df.select(self.outputCols)
        return df
