from os import listdir
from os import path
import gzip
import json
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
import argparse
import logging
import tarfile

logging.basicConfig(filename='conversion.log',level=logging.DEBUG)

class Converter(object):
    """docstring for Converter"""
    def __init__(self, inputfolder, outputfolder, style, source, compressed=None):
        self.inputfolder = inputfolder
        self.outputfolder = outputfolder
        self.style = self.load_style(style)
        self.source = source
        if compressed:
            self.compressed = compressed

    def load_style(self, style):
        with open(".".join([style,"json"]), "r") as infile:
            style = json.load(infile)
        return style

    def metadata2dict(self, elementlist):
        d = {}
        for elem in elementlist:
            if elem.attrib != {}:
                if elem.text:
                    d[next(iter(elem.attrib.values()))] = elem.text
                else:
                    d.update(elem.attrib)
            else:
                if elem.text:
                    d[elem.tag] = elem.text
        return d

    def get_dates(self, article):
        try:
            coll_date = article.find(self.style.get("coll_date"))
            date_coll = coll_date.find('year').text
        except:
            date_coll = None

        try:
            epub_date = article.find(self.style.get("coll_date"))
            date_epub = "-".join([epub_date.find('year').text, epub_date.find('month').text, epub_date.find('day').text])
        except:
            date_epub = None
        return date_coll, date_epub

    def get_disciplines(self, article):
        disciplines = []
        for elem in article.findall(self.style.get("disciplines"))[:50]:
            discipline = []
            while elem.find('subj-group'):
                discipline.append(elem.findtext('subject', ""))
                elem = elem.find('subj-group')
                if elem is None:
                    break
            #print("-".join(discipline))
            #"-".join(discipline).split("-"))
            disciplines.append(discipline)
        return disciplines

    def get_metadata(self, article):
        meta_journal = self.metadata2dict(article.findall(self.style.get("meta_journal")))
        meta_article = self.metadata2dict(article.find(self.style.get("meta_article")).getchildren())
        metadata = dict(meta_journal, **meta_article)
        date_coll, date_epub = self.get_dates(article)

        # add additional, manual identified metadata
        if date_coll:
            metadata["date_coll"] = date_coll
        if date_epub:
            metadata["electronicPublicationDate"] = date_epub
        metadata['title'] = article.findtext('.//article-title', None)
        metadata['article-subject-type'] = article.findtext('.//article-categories//subj-group[@subj-group-type="heading"]/subject', None)
        metadata['disciplines'] = self.get_disciplines(article)
        metadata['keywordList'] = [e.text for e in article.findall(self.style.get('keywordList'))]
        metadata['cprojectID'] = metadata.get('pmcid')
        return metadata

    def get_texts(self, article):
        ps = article.findall(self.style.get("fulltext"))
        ps_abs = article.findall(self.style.get("abstract"))
        text = " ".join("".join(p.itertext()) for p in ps)
        text_abs = " ".join("".join(p.itertext()) for p in ps_abs)
        return {"fulltext":text, "abstract":text_abs}

    def create_json(self, article):
        metadata = self.get_metadata(article)
        texts = self.get_texts(article)
        article_json = dict(metadata, **texts)
        article_json = {k.split("}")[1] if k.startswith("{") else k:v for k,v in article_json.items()} # filter out xml-namespaces
        article_json = {k:v for k,v in article_json.items() if not "/" in k} # filter out urls and dois
        return article_json

    def corexml2json(self, xmlfilepath):
        head, tail = path.split(xmlfilepath)
        filename = path.splitext(path.splitext(tail)[0])[0]
        if path.isfile(path.join(self.outputfolder, filename+".json")):
            return
        try:
            with gzip.open(xmlfilepath, "r") as infile:
                tree = ET.parse(infile)
                root = tree.getroot()
        except Exception as e:
            logging.exception(" ".join([str(e), xmlfilepath]))

        try:
            for article in root.getchildren():
                with open(path.join(self.outputfolder, filename+".json"), "a") as outfile:
                    jsonfile = self.create_json(article)
                    #json.dump(jsonfile, outfile, allow_nan=False)
                    #outfile.write("\n")
                    outfile.write(json.dumps(jsonfile)+"\n")
        except Exception as e:
            logging.exception(" ".join([str(e), xmlfilepath]))

    def plosxml2json(self, xmlfilepath):
        head, tail = path.split(xmlfilepath)
        filename = path.splitext(path.splitext(tail)[0])[0]
        if path.isfile(path.join(self.outputfolder, filename+".json")):
            return
        i = 0
        if self.compressed == "gz":
            with tarfile.open(xmlfilepath, "r:gz") as infile:
                infile = infile
        else:
            with open(xmlfilepath, "r") as infile:
                infile = infile
        for line in infile.readlines()[:10]:
            if line.startswith("<article"):
                break
            i+=1
        article = infile.readlines()[i:]
        article = "".join(article)
        try:
            article = ET.fromstring(article)

            with open(path.join(self.outputfolder, filename+".json"), "a") as outfile:
                jsonfile = self.create_json(article)
                #json.dump(jsonfile, outfile, allow_nan=False)
                #outfile.write("\n")
                outfile.write(json.dumps(jsonfile)+"\n")
        except Exception as e:
            logging.exception(" ".join([str(e), xmlfilepath]))

    def core2json(self):
        files = [f for f in listdir(self.inputfolder) if path.isfile(path.join(self.inputfolder, f))]
        for file in files:
            self.corexml2json(path.join(self.inputfolder, file))

    def plos2json(self):
        files = [f for f in listdir(self.inputfolder) if path.isfile(path.join(self.inputfolder, f))]
        for file in files:
            self.plosxml2json(path.join(self.inputfolder, file))

def main(args):
    converter = Converter(args.inputfolder, args.outputfolder, args.style, args.source, args.compressed)
    if args.source in ['CORE', 'EUPMC']:
        converter.core2json()
    if args.source in ['PLOS']:
        converter.plos2json()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert publisher collections to JSON dumps ready for spark-input')
    parser.add_argument('--style', dest='style', help='style of the XML source, one of [JATS]')
    parser.add_argument('--source', dest='source', help='from which publisher the collection comes from, one of [CORE, PLOS, EUPMC]')
    parser.add_argument('--input', dest='inputfolder', help='relative or absolute path of the input folder')
    parser.add_argument('--output', dest='outputfolder', help='relative or absolute path of the output folder')
    parser.add_argument('--compressed', dest='compressed', help='flag if inputfiles are compressed')
    args = parser.parse_args()
    main(args)
