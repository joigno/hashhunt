import os.path
import requests

from download import download_wikipedia_abstracts
from load import load_documents
from search.timing import timing
from search.index import Index


@timing
def index_documents(documents, index):
    for i, document in enumerate(documents):
        index.index_document(document)
        if i % 10 == 0:
            print(f'Indexed {i} documents', end='\r')
            index.documents.commit()
            index.index.commit()
    return index


if __name__ == '__main__':
    # this will only download the xml dump if you don't have a copy already;
    # just delete the file if you want a fresh copy
    #if not os.path.exists('data/enwiki-latest-abstract.xml.gz'):
    #    download_wikipedia_abstracts()

    index = index_documents(load_documents(), Index())
    print(f'Index contains {len(index.documents)} documents')

    print(index.search('decaf', search_type='AND'))
    print(index.search('0xdecaf', search_type='AND'))
