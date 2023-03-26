from elasticsearch7 import Elasticsearch
import Utils as Utils

INDEX = 'homework3'
CLOUD_ID = 'homework3:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ3NjJhZDM3NTU4MTY0OWM1ODM3ZTRiYjg5NjI5ZmFiNyQyMWU0ZDM1MDQzNmY0NDA3OGIzZTY0NTMyN2Q0NTUzNg=='
es = Elasticsearch(request_timeout = 1000, cloud_id = CLOUD_ID, http_auth= ('elastic', '74QCRsRmX0WpC67mIj0PZfDw'))

def normalize_link_formatting(doc, in_or_out_links):
    links = doc['_source'][in_or_out_links]

    if type(links) is not list:
        links_list = links.split(", ")
        links = links_list

    return links

def generate_link_dicts():
    inlinks = {}
    outlinks = {}

    q = {
        'query':{
            'match_all':{}
        }
    }

    all_docs = es.search(index=INDEX, body=q, size=97259, scroll='3m', request_timeout=500)

    for doc in all_docs['hits']['hits']:
        inlinks[doc['_id']] = normalize_link_formatting(doc, "inlinks")
        outlinks[doc['_id']] = normalize_link_formatting(doc, "outlinks")

    return inlinks, outlinks

"""ella1 = 'https://en.wikipedia.org/wiki/Posidonius'
ella2 = 'https://en.wikipedia.org/wiki/Der_Blaue_Reiter'
olivia1 = 'http://raceforward.org/research/mkrajcer'
olivia2 = 'https://commons.wikimedia.org/w/index.php?campaign=loginCTA&returnto=Special:WhatLinksHere/Category:Critical+race+theory&returntoquery=target=Category%253ACritical+race+theory&title=Special:CreateAccount'
all = 'https://en.wikipedia.org/wiki/Wikipedia:Disambiguation'

ella1inlinks = normalize_link_formatting(ella1, 'inlinks')
ella1outlinks = normalize_link_formatting(ella1, 'outlinks')
olivia1in = normalize_link_formatting(olivia1, 'inlinks')
olivia1out = normalize_link_formatting(olivia1, 'outlinks')
allin = normalize_link_formatting(all, 'inlinks')
allout = normalize_link_formatting(all, 'outlinks')

ella2inlinks = normalize_link_formatting(ella2, 'inlinks')
ella2outlinks = normalize_link_formatting(ella2, 'outlinks')
olivia2in = normalize_link_formatting(olivia2, 'inlinks')
olivia2out = normalize_link_formatting(olivia2, 'outlinks')

print(ella1inlinks)
print(ella1outlinks)
print(olivia1in)
print(olivia1out)
print(ella2inlinks)
print(ella2outlinks)
print(olivia2in)
print(olivia2out)
print(allin)
print(allout)"""

if __name__ == "__main__":
    utils = Utils()
    inlinks, outlinks = generate_link_dicts()
    utils.save_dict("inlinks.pkl", inlinks)
    utils.save_dict("outlinks.pkl", outlinks)
