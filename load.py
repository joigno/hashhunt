import gzip
from lxml import etree
import time
from ast import literal_eval

from search.documents import Abstract

def load_documents():
    start = time.time()
    #with gzip.open('data/sample.xml.gz', 'rb') as f:
    #with open('data/sample-100k.xml', 'rb') as f:
    with open('data/eth-100k.xml', 'rb') as f:
        #doc_id = 0
        for _, element in etree.iterparse(f, events=('end',), tag='doc'):
            title = element.findtext('./title')
            url = element.findtext('./url')
            abstract = element.findtext('./abstract')
            doc_id = literal_eval(title) % 999999999999
            title = title.replace('0x','')

            yield Abstract(ID=doc_id, title=title, url=url)

            #doc_id += 1
            element.clear()
    end = time.time()
    print(f'Parsing XML took {end - start} seconds')
