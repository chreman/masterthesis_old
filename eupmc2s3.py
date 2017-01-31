# download XMLs
# run extractor
# write to S3 bucket


from urllib import request
from bs4 import BeautifulSoup
import os
from os import path
from xml2json import Converter
import json
import gzip
import boto3
import logging
import sys

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)
formatter = logging.Formatter('%(asctime)-15s %(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger = logging.getLogger('sparklogger')
logger.addHandler(ch)

logger.info('Beginning workflow')


def get_eupmc_urls():
    url = "http://europepmc.org/ftp/oa/"
    r = request.urlopen(url)
    soup = BeautifulSoup(r.read())
    hrefs = soup.find_all("a", href=True)
    eupmcfiles = [h.text for h in hrefs if h.text.endswith(".xml.gz")]
    return eupmcfiles

def download_xml(url, outputfolder):
    logger.info("Downloading %s" %url)
    head, tail = path.split(url)
    r = request.urlopen(url)
    content = r.read()
    with open(path.join(outputfolder, tail), "wb") as outfile:
        outfile.write(content)

def convert_files(inputfolder):
    logger.info("Converting...")
    conv = Converter(inputfolder, "jsons", "JATS", "EUPMC", "gz")
    conv.core2json()

def compress_files(sourcefolder, outdir):
    for f in os.listdir(sourcefolder):
        sourcepath = path.join(sourcefolder, f)
        with open(sourcepath, "rb") as infile:
            sourcedata = infile.read()

        targetpath = path.join(outdir, f)+".gz"
        logger.info("Compressing %s to %s." %(sourcepath, targetpath))
        with gzip.open(targetpath, mode="wb") as outfile:
            outfile.write(sourcedata)

def write_files_S3(sourcefolder, s3):
    for f in os.listdir(sourcefolder):
        sourcepath = path.join(sourcefolder, f)
        targetpath = path.join("eupmc",f)
        logger.info("Writing %s to S3 as %s." %(sourcepath, targetpath))
        s3.Object('data-ck', targetpath).put(Body=open(sourcepath, 'rb'))

def clean_up(folder):
    """Clean up intermediate files so that the instance disk doesn't clog."""
    for f in os.listdir(folder):
        os.remove(path.join(folder, f))

def get_cached(s3):
    """Load the list of visited urls for caching purposes.

    :returns: set of str"""
    bucket = s3.Bucket('data-ck')
    visited = []
    for obj in bucket.objects.filter(Prefix='eupmc/P'):
        visited.append(obj.key)
    visited = [path.split(v)[1] for v in visited]
    visited = [path.splitext(v)[0] for v in visited]
    visited = [path.splitext(v)[0] for v in visited]
    return set(visited)

def main():
    eupmcfiles = get_eupmc_urls()
    baseurl = "http://europepmc.org/ftp/oa/"
    for p in ["cache", "jsons", "gzfiles"]:
        if not path.exists(p):
            os.makedirs(p)
    s3 = boto3.resource('s3')
    visited = get_cached(s3)
    for i, xml in enumerate(eupmcfiles):
        logger.info('%d/%d' %(i,len(eupmcfiles)))
        if path.splitext(path.splitext(xml)[0])[0] in visited:
            continue
        try:
            download_xml(path.join(baseurl+xml), "cache")
        except:
            logger.exception(xml)
        convert_files("cache")
        compress_files("jsons", "gzfiles")
        write_files_S3("gzfiles", s3)
        logger.info("Cleaning up after %s" %xml)
        clean_up("cache")
        clean_up("jsons")
        clean_up("gzfiles")
        with open("visited_links.txt", "a") as outfile:
            outfile.write(xml+"\n")

if __name__ == '__main__':
    main()
