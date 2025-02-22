import math, pickle

from .timing import timing
from .analysis import analyze
from .filedict import FileDict



class Index:
    def __init__(self, persistent=True):
        self.persistent = persistent
        self.index = {}
        self.documents = {}
        
    def __del__(self):
        #print('CLOSING...')
        if self.persistent:
            #self.index.close() # close is calling sync() inside.
            #self.documents.close()
            pass

    def index_document(self, document):
        if document.ID not in self.documents:
            self.documents[document.ID] = document
            document.analyze()

        for token in analyze(document.fulltext):
            if token not in self.index:
                self.index[token] = set()
            self.index[token].add(document.ID)

    def document_frequency(self, token):
        return len(self.index.get(token, set()))

    def inverse_document_frequency(self, token):
        # Manning, Hinrich and Schütze use log10, so we do too, even though it
        # doesn't really matter which log we use anyway
        # https://nlp.stanford.edu/IR-book/html/htmledition/inverse-document-frequency-1.html
        if self.document_frequency(token) == 0:
            return 0.0
        return math.log10(len(self.documents) / self.document_frequency(token))

    def _results(self, analyzed_query):
        ret_val = [self.index[token] for token in analyzed_query]
        return [l for l in ret_val if l]

    def persist_to_disk(self):
        if self.persistent:
            disk_docs = FileDict('db/documents')
            for k in self.documents:
                disk_docs[k] = self.documents[k]
            self.documents = {}
            disk_index = FileDict('db/index')
            disk_index.extend_sets( self.index )        
            self.index = {}

    def __len__(self):
        if self.persistent:
            return len(FileDict('db/documents'))
        else:
            return len(self.index)

    @timing
    def search(self, query, search_type='AND', rank=False):
        """
        Search; this will return documents that contain words from the query,
        and rank them if requested (sets are fast, but unordered).

        Parameters:
          - query: the query string
          - search_type: ('AND', 'OR') do all query terms have to match, or just one
          - score: (True, False) if True, rank results based on TF-IDF score
        """
        if search_type not in ('AND', 'OR'):
            return []

        if self.persistent:
            self.documents = FileDict('db/documents')
            self.index = FileDict('db/index')

        query = query.replace('0x', '')
        analyzed_query = analyze(query)
        results = self._results(analyzed_query)
        if results == []:
            return []
        if search_type == 'AND':
            # all tokens must be in the document
            documents = [self.documents[doc_id] for doc_id in set.intersection(*results)]
        if search_type == 'OR':
            # only one token has to be in the document
            documents = [self.documents[doc_id] for doc_id in set.union(*results)]
        if rank:
            return self.rank(analyzed_query, documents)
        return documents

    def rank(self, analyzed_query, documents):
        results = []
        if not documents:
            return results
        for document in documents:
            score = 0.0
            for token in analyzed_query:
                tf = document.term_frequency(token)
                idf = self.inverse_document_frequency(token)
                score += tf * idf
            results.append((document, score))
        return sorted(results, key=lambda doc: doc[1], reverse=True)
