import math
import os
import random
import sys

from Utils import Utils
from elasticsearch7 import Elasticsearch

INDEX = 'homework3'
CLOUD_ID = 'homework3:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ3NjJhZDM3NTU4MTY0OWM1ODM3ZTRiYjg5NjI5ZmFiNyQyMWU0ZDM1MDQzNmY0NDA3OGIzZTY0NTMyN2Q0NTUzNg=='
es = Elasticsearch(request_timeout = 1000, cloud_id = CLOUD_ID, http_auth= ('elastic', '74QCRsRmX0WpC67mIj0PZfDw'))
utils = Utils()

MERGED_INLINKS = "/Users/ellataira/Desktop/is4200/homework-4-ellataira/Code/data/merged_inlinks.pkl"
MERGED_OUTLINKS = "/Users/ellataira/Desktop/is4200/homework-4-ellataira/Code/data/merged_outlinks.pkl"

class HITS:

    # instance of HITS can use either saved base_set pkl file, or regenerate base_set
    def __init__(self, query, base_set=None):
        self.query = self.query_analyzer(query)
        print(self.query)
        self.root_set = None
        self.inlinks = utils.read_pickle(MERGED_INLINKS)
        self.outlinks = utils.read_pickle(MERGED_OUTLINKS)
        print(len(self.inlinks))
        print(len(self.outlinks))
        self.base_set = base_set
        if not base_set:
            self.get_root_set()
            self.expand_root_to_base()

    # modify input queries through stemming, shifting to lowercase, and removing stop words
    # stores the modified queries in a dictionary as key-value : (qID, query)
    def query_analyzer(self, query):
        body = {
            "tokenizer": "standard",
            "filter": ["porter_stem", "lowercase"],
            "text": query
        }

        res = es.indices.analyze(body=body, index=INDEX)
        return [list["token"] for list in res["tokens"]]

    # using ES builtin retrieval model to get 1000 root documents
    def get_root_set(self):
        body = {
            "size":1000,
            "query": {
                "match": {'content': " ".join(self.query)}
            }
        }

        res_es_search = es.search(index=INDEX, body=body)
        self.root_set = res_es_search

    # iterate over root set to build up base set of 10000 documents using in/outlinks from root docs
    def expand_root_to_base(self):
        base_size = 10000
        base_set = set()
        d = 200

        base_set.update(d['_id'] for d in self.root_set['hits']['hits'])

        print(len(base_set))

        # while len(base_set) <= base_size:
        for doc in self.root_set['hits']['hits']:
            if len(base_set) <= base_size:
                to_append = set()
                docid = doc['_id']
                print(docid)
                print(len(base_set))

                inlinks= self.inlinks[docid]
                outlinks= self.outlinks[docid]

                to_append.update(outlinks)

                if len(inlinks) <= d :
                    inlinks_to_add = inlinks
                else:
                    inlinks_to_add = random.sample(inlinks, k=d)

                to_append.update(inlinks_to_add)

                to_append = list(to_append)

                for a in to_append: # ensure all docs in base_set are present in the index
                    try:
                        check_ins = self.inlinks[a]
                        check_outs = self.outlinks[a]
                    except:
                        to_append.remove(a)
                        print("removed")

                if (len(base_set) + len(to_append)) > base_size: # if there are too many new links to add (i.e. total over 10000,
                                                                # only add until 10,000 total and break
                    to_append = list(to_append)[:(base_size - len(base_set))]

                base_set.update(to_append)
            else:
                break

        print("final size: " + str(len(base_set)))
        self.base_set = base_set
        utils.save_dict("base_set.pkl", base_set)

    # compute HITS scores from base_sets
    # returns HITS dictionary which maps docid: (authority score, hub score)
    def compute_hits(self):
        hits = {} #hits dict maps docid : (authority score, hub score)
        perplexity_stable_count = 0
        old_auth_p, old_hub_p = None, None

        for doc in self.base_set:
            hits[doc] = (1,1)

        while perplexity_stable_count < 4:
            for docid, scores in hits.items():
                new_auth = self.update_authority_scores(hits, docid)
                new_hub = self.update_hub_scores(hits, docid)

                hits[docid] = (new_auth, new_hub)

            hits = self.normalize(hits)

            new_auth_p, new_hub_p = self.calc_perplexities(hits)
            print("new auth perplexity: " + str(new_auth_p))
            print("new hub perplexity: " + str(new_hub_p))

            if self.is_converged(new_auth_p, old_auth_p) and self.is_converged(new_hub_p, old_hub_p):
                perplexity_stable_count += 1

            old_auth_p, old_hub_p = new_auth_p, new_hub_p

        return hits

    # update authority score based on given doc's inlinks' hub scores
    def update_authority_scores(self, hits, docid):
        sum = 1
        try:
            ins = self.inlinks[docid]
        except:
            ins = []
        for i in ins:
            try:
                auth, hub = hits[i]
                print("found")
            except:
                print("not found in hits")
                auth, hub = 0,0
            sum += hub

        print(sum)
        return sum

    # update hub score based on given doc's outlinks' auth scores
    def update_hub_scores(self, hits, docid):
        sum = 1
        try:
            outs = self.outlinks[docid]
        except:
            outs = []
        for o in outs:
            try:
                auth, hub = hits[o]
                print("found")
            except:
                print("not found in hits")
                auth, hub = 0,0

            sum += auth
        print(sum)
        return sum

    # normalize authority and hub scores after each iteration
    def normalize(self, hits):
        auth_norm = 0
        hub_norm = 0

        for docid, scores in hits.items():
            auth, hub = scores
            auth_norm += auth**2
            hub_norm += hub**2

        for docid, scores in hits.items():
            old_auth, old_hub = hits[docid]
            hits[docid] = (old_auth / auth_norm, old_hub / hub_norm)

        return hits

    # determines if scores are converged by comparing perplexity scores
    def is_converged(self, new_p, old_p):
        if old_p != None:
            diff = math.fabs(new_p-old_p)
            print("perplexity diff: " + str(diff))
            if diff <= 0.00000001 :
                return True
        return False

    # calculates perplexity values of authority and hub scores
    def calc_perplexities(self, hits):
        auth_entropy = 0
        hub_entropy = 0
        for url, scores in hits.items():
            auth, hub = scores
            auth_entropy += auth * math.log(auth, 2)
            hub_entropy += hub * math.log(hub, 2)

        auth_p = math.pow(2, -1 * auth_entropy)
        hub_p = math.pow(2, -1 * hub_entropy)
        return auth_p, hub_p

    # saves top 500 documents by authority and hub scores in two separate txt files
    def save_auth_and_hub_scores(self, hits):
        to_save_auth = self.sort_descending(hits, 500, 0)
        to_save_hub = self.sort_descending(hits, 500, 1)

        self.save_top_500("authority_scores1.txt", to_save_auth, 0)
        self.save_top_500("hub_scores1.txt", to_save_hub, 1)

    # saves top 500 documents in given dictionary
    def save_top_500(self, filename, to_save, keyindex):
        file = "/Users/ellataira/Desktop/is4200/homework-4-ellataira/Results/" + filename

        if os.path.exists(file):
            os.remove(file)

        with open(file, 'w') as f:
            for link, score in to_save:
                print(score)
                s = score[keyindex]
                f.write(link + "\t" + str(s) + "\n")

        f.close()

    # sorts dictionary by specified key
    def sort_descending(self, pagerank_dict, k, key_index):
        sorted_docs = sorted(pagerank_dict.items(), key=lambda item: item[1][key_index], reverse=True)
        del sorted_docs[k:]
        return sorted_docs

if __name__ == "__main__":
    query = "Social justice Movements"
    base_set = utils.read_pickle("/Users/ellataira/Desktop/is4200/homework-4-ellataira/Code/data/base_set.pkl")
    hits = HITS(query, base_set=base_set)
    # hits = HITS(query)
    hit_scores = hits.compute_hits()
    hits.save_auth_and_hub_scores(hit_scores)