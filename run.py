import os.path
import requests
import multiprocessing

from download import download_wikipedia_abstracts
from load import load_documents
from search.timing import timing
from search.index import Index
from search.documents import Abstract


@timing
def index_documents(documents, index):
    for i, document in enumerate(documents):
        index.index_document(document)
        if i % 10000 == 0:
            print(f'Indexed {i} documents', end='\r')
            #print('Downloading to disk...', end='\r')
            print(f'Starting disk persistence (total {i} documents) ...', end='\r')
            index.persist_to_disk()
            print(f'Ending disk persistence (total {i} documents) ...', end='\r')
        if i == 50000:
            break
    return index


def index_multip(ID: int, title: str, abstract: str, url: str):
    new_index = Index()
    doc = Abstract(ID=ID, title=title, url=url, abstract=abstract)
    new_index.index_document(doc)

@timing
def index_documents_multip(documents, index):
    procs = []
    for i, document in enumerate(documents):
        # https://www.simplilearn.com/tutorials/python-tutorial/multiprocessing-in-python#:~:text=Multiprocessing%20in%20Python%20is%20a,threads%20that%20can%20run%20independently.
        proc = multiprocessing.Process(target=index_multip, args=(document.ID, document.title, document.abstract, document.url, ))
        proc.start()
        procs.append( proc )
        #index.index_document(document)
        if i % 100 == 0:
            for p in procs:
                p.join()
            print(f'Indexed {i} documents MULTIPROCESSING', end='\r')
            procs = []
        #if i == 2500:
        #    break
    return index


if __name__ == '__main__':
    # this will only download the xml dump if you don't have a copy already;
    # just delete the file if you want a fresh copy
    #if not os.path.exists('data/enwiki-latest-abstract.xml.gz'):
    #    download_wikipedia_abstracts()

    index = index_documents(load_documents(), Index())
    print(f'Index contains {len(index)} documents')

    print(index.search('1acec', search_type='AND'))
    print(index.search('522d5', search_type='AND'))
    print(index.search('0xbadcafe', search_type='AND'))
