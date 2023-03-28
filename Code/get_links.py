from elasticsearch7 import Elasticsearch
import Utils

INDEX = 'homework3'
CLOUD_ID = 'homework3:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ3NjJhZDM3NTU4MTY0OWM1ODM3ZTRiYjg5NjI5ZmFiNyQyMWU0ZDM1MDQzNmY0NDA3OGIzZTY0NTMyN2Q0NTUzNg=='
es = Elasticsearch(request_timeout = 1000, cloud_id = CLOUD_ID, http_auth= ('elastic', '74QCRsRmX0WpC67mIj0PZfDw'))

def normalize_link_formatting(doc, in_or_out_links):
    links = doc['_source'][in_or_out_links]

    if type(links) is not list:
        links_list = links.split(", ")
        links = links_list

    return links

def generate_link_dicts_from_es():
    inlinks = {}
    outlinks = {}
    doc_count = 0

    q = {
        'query':{
            'match_all':{}
        }
    }

    resp = es.search(index=INDEX, body=q, size=2000, scroll='3m')
    old_scroll_id = resp['_scroll_id']

    while len(resp['hits']['hits']):
        resp = es.scroll(scroll_id=old_scroll_id, scroll='3m')

        if old_scroll_id != resp['_scroll_id']:
            print("new scroll id: " + resp['_scroll_id'])

        old_scroll_id = resp['_scroll_id']

        print('response["hits"]["total"]["value"]:', resp["hits"]["total"]["value"])

        for doc in resp['hits']['hits']:
            inlinks[doc['_id']] = normalize_link_formatting(doc, "inlinks")
            outlinks[doc['_id']] = normalize_link_formatting(doc, "outlinks")
            doc_count+=1
            print(doc_count)

    print(doc_count)
    return inlinks, outlinks

def generate_link_dicts_from_txt():
    file = "/Users/ellataira/Desktop/is4200/homework-4-ellataira/wt2g_inlinks.txt"
    inlink_dict = {}
    outlink_dict = {}

    with open(file) as opened:
        for line in opened:
            split_lines = line.split()
            inlink_dict[split_lines[0]] = list(set(split_lines[1:]))
            outlink_dict.update({each: outlink_dict.get(each, []) + [split_lines[0]] for each in split_lines[1:]})

    outlink_dict.update({key: [] for key in set(inlink_dict.keys()) - set(outlink_dict.keys())})
    inlink_dict = {key: list(value) for key, value in inlink_dict.items()}
    outlink_dict = {key: list(set(value)) for key, value in outlink_dict.items()}

    print(len(inlink_dict))
    print(len(outlink_dict))
    return inlink_dict, outlink_dict


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
    utils = Utils.Utils()
    # merged_inlinks, merged_outlinks = generate_link_dicts_from_es()
    # utils.save_dict("data/merged_inlinks.pkl", merged_inlinks)
    # utils.save_dict("data/merged_outlinks.pkl", merged_outlinks)
    txt_inlinks, txt_outlinks = generate_link_dicts_from_txt()
    utils.save_dict("data/txt_inlinks.pkl", txt_inlinks)
    utils.save_dict("data/txt_outlinks.pkl", txt_outlinks)

