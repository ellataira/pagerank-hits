import math
import os

from Utils import Utils
MERGED_INLINKS = "/Users/ellataira/Desktop/is4200/homework-4-ellataira/Code/data/merged_inlinks.pkl"
MERGED_OUTLINKS = "/Users/ellataira/Desktop/is4200/homework-4-ellataira/Code/data/merged_outlinks.pkl"
TXT_INLINKS = "/Users/ellataira/Desktop/is4200/homework-4-ellataira/Code/data/txt_inlinks.pkl"
TXT_OUTLINKS = "/Users/ellataira/Desktop/is4200/homework-4-ellataira/Code/data/txt_outlinks.pkl"
utils = Utils()
class PageRank:
    def __init__(self, inlink_file, outlink_file, d=0.85):
        self.inlinks = utils.read_pickle(inlink_file)
        self.outlinks = utils.read_pickle(outlink_file)
        self.sink_nodes = self.get_sink_nodes(self.outlinks)
        self.n = len(self.inlinks)
        self.delta = d

    # gets all sink nodes, i.e. nodes with 0 outlinks
    def get_sink_nodes(self, outlinks):
        sink_list = []
        for url, links in outlinks.items():
            if len(links) == 0:
                sink_list.append(url)
        return sink_list

    # calculates the pagerank scores of a graph until convergence
    def calc_pagerank(self):
        pagerank_dict = {}
        new_pr_dict = {}
        perplexity_stable_count = 0
        old_perplexity = None
        d = self.delta
        N = self.n

        for docid, inlinks in self.inlinks.items():
            pagerank_dict[docid] = 1 / len(self.inlinks)

        while perplexity_stable_count < 4:
            sink = 0
            for docid in self.sink_nodes:
                sink += pagerank_dict[docid]
            for docid, links in self.inlinks.items():
                new_pr_dict[docid] = (1 - d) / N
                new_pr_dict[docid] += d * sink / N

                for l in links:
                    try: # if the inlink is not a document in the corpus, set to 0, which will make the score = 0
                        pr_l = pagerank_dict[l]
                    except:
                        pr_l = 0

                    len_outlinks = len(self.try_get_links(l, self.outlinks))
                    if len_outlinks != 0: #
                        # len_outlinks == 0 when the inlink, l, has no outlinks, but cannot divide by 0,
                        # so only add to score if docid has valid outlinks
                        new_pr_dict[docid] += d * pr_l / len_outlinks

            for docid, score in pagerank_dict.items():
                pagerank_dict[docid] = new_pr_dict[docid]

            new_perplexity = self.calc_perplexity(pagerank_dict)
            print("perplexity: " + str(new_perplexity))

            if self.is_converged(new_perplexity, old_perplexity):
                perplexity_stable_count +=1

            old_perplexity = new_perplexity

        return pagerank_dict

    # saves top 500 pageranked documents to txt file
    def save_top_500(self, pagerank_dict, filename):
        to_save = self.sort_descending(pagerank_dict, 500)
        file = "/Users/ellataira/Desktop/is4200/homework-4-ellataira/Results/" + filename

        if os.path.exists(file):
            os.remove(file)

        with open(file, 'w') as f:
            for link, score in to_save:
                no_outlinks = len(self.try_get_links(link, self.outlinks))
                no_inlinks = len(self.try_get_links(link, self.inlinks))

                f.write(link + " " + str(score) + " " + str(no_outlinks) + " " + str(no_inlinks) + "\n")

        f.close()

    # helper methods to get in/outlinks with given url key, if they exist
    def try_get_links(self, url, in_or_out):
        try:
            ret = in_or_out[url]
        except:
            ret = []
        return ret

    # sorts a given dictionary in descending order by score
    def sort_descending(self, pagerank_dict, k):
        sorted_docs = sorted(pagerank_dict.items(), key=lambda item: item[1], reverse=True)
        del sorted_docs[k:]
        return sorted_docs

    # determines if scores have converged based on perplexity values
    def is_converged(self, new_p, old_p):
        if old_p != None:
            diff = math.fabs(new_p-old_p)
            print("perplexity diff: " + str(diff))
            if diff <= 0.00000001 :
                return True
        return False

    # calculates perplexity value of pagerank scores
    def calc_perplexity(self, pagerank_dict):
        entropy = 0
        for url, score in pagerank_dict.items():
            entropy += score * math.log(score, 2)
        entropy = -1 * entropy
        p = math.pow(2, entropy)
        return p


if __name__ == "__main__":
    pr = PageRank(inlink_file=MERGED_INLINKS, outlink_file=MERGED_OUTLINKS)
    print(len(pr.inlinks))
    print(len(pr.outlinks))
    pageranked_dict = pr.calc_pagerank()
    pr.save_top_500(pageranked_dict, "merged_pagerank500.txt")

    # pr = PageRank(inlink_file=TXT_INLINKS, outlink_file=TXT_OUTLINKS)
    # print(len(pr.inlinks))
    # print(len(pr.outlinks))
    # pageranked_dict = pr.calc_pagerank()
    # pr.save_top_500(pageranked_dict, "txt_pagerank500.txt")