
from elasticsearch7 import Elasticsearch
import Utils

INDEX = 'homework3'
CLOUD_ID = 'homework3:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ3NjJhZDM3NTU4MTY0OWM1ODM3ZTRiYjg5NjI5ZmFiNyQyMWU0ZDM1MDQzNmY0NDA3OGIzZTY0NTMyN2Q0NTUzNg=='
es = Elasticsearch(request_timeout = 1000, cloud_id = CLOUD_ID, http_auth= ('elastic', '74QCRsRmX0WpC67mIj0PZfDw'))

# normalizes formatting of inlinks from ES merged index discrepancies
def normalize_link_formatting(doc, in_or_out_links):
    links = doc['_source'][in_or_out_links]

    if type(links) is not list:
        links_list = links.split(", ")
        links = links_list

    return links

# generates inlink and outlink graph of all documents in ES index
def generate_link_dicts_from_es():
    inlinks = {}
    outlinks = {}
    doc_count = 0

    q = {
        'query':{
            'match_all':{}
        }
    }

    resp = es.search(index=INDEX, body=q, size=1000, scroll='3m')
    old_scroll_id = resp['_scroll_id']

    for doc in resp['hits']['hits']:
        inlinks[doc['_id']] = normalize_link_formatting(doc, "inlinks")
        outlinks[doc['_id']] = normalize_link_formatting(doc, "outlinks")
        doc_count += 1

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
    print(len(inlinks))
    print(len(outlinks))
    return inlinks, outlinks

# generates inlink and outlink graph from wt2g_inlinks.txt file
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


if __name__ == "__main__":
    utils = Utils.Utils()
    merged_inlinks, merged_outlinks = generate_link_dicts_from_es()
    utils.save_dict("data/merged_inlinks.pkl", merged_inlinks)
    utils.save_dict("data/merged_outlinks.pkl", merged_outlinks)
    # txt_inlinks, txt_outlinks = generate_link_dicts_from_txt()
    # utils.save_dict("data/txt_inlinks.pkl", txt_inlinks)
    # utils.save_dict("data/txt_outlinks.pkl", txt_outlinks)

