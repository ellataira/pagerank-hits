# Web Graph Computation (PageRank and HITS)

The objective of this project is to compute link graph measures for a set of crawled documents using the adjacency matrix.

## `get_links.py`

This file generates the adjacency matrices of inlinks and outlinks for all crawled documents stored in an Elasticsearch index. 

## `pagerank.py`

This file calculates the PageRank scores of each document in the index until convergence. Scores have converged if their perplexity scores remain consistent for 4 iterations. 

The implemented algorithm follows the following [pseudocode](https://course.ccs.neu.edu/cs6200f13/proj1.html):

```
// P is the set of all pages; |P| = N
// S is the set of sink nodes, i.e., pages that have no out links
// M(p) is the set of pages that link to page p
// L(q) is the number of out-links from page q
// d is the PageRank damping/teleportation factor; use d = 0.85 as is typical

foreach page p in P
  PR(p) = 1/N                          /* initial value */

while PageRank has not converged do
  sinkPR = 0
  foreach page p in S                  /* calculate total sink PR */
    sinkPR += PR(p)
  foreach page p in P
    newPR(p) = (1-d)/N                 /* teleportation */
    newPR(p) += d*sinkPR/N             /* spread remaining sink PR evenly */
    foreach page q in M(p)             /* pages pointing to p */
      newPR(p) += d*PR(q)/L(q)         /* add share of PageRank from in-links */
  foreach page p
    PR(p) = newPR(p)

return PR
```

Upon convergence, the top 500 ranked documents are outputted into a text file that records the document ID, document PageRank score, number of outlinks, and number of inlinks. 

## `hits.py`

This file computes the Hub and Authority scores for the same set of crawled documents. To do so, we first obtain a root set of the top 1000 relevant documents using the Elasticsearch built-in retrieval method. The root set is then expanded 
into a base set of 10,000 documents by adding each root document's outlinks and 200 of its inlinks. Then, the hub and authority scores are computed for the documents in the base set. 
A page's authority score is equal to the sum of the hub scores of each of its inlinks. A page's hub score is equal to the sum of the authority scores of its outlinks. HITS scores are normalized at every iteration. Same as PageRank, HITS scores are considered to be converged when their perplexity values remain consistent for 4 iterations. 

Upon convergence, the top 500 hub webpages and top 500 authority webpages are outputted to a text file that records 
the document ID and hub/authority score. 


### Discussion: 

Some pages with low inlink counts still have higher PageRank scores than pages with more inlinks because they have inlinks of higher quality. If a document has an inlink with a very high PageRank score, then
it will have a higher updated score. In the PageRank algorithm, score has a positive relationship with its inlinks' scores; higher scored inlinks will lead to a higher scored document. 

For example, consider the following two documents, ranked 51 and 52: 

[rank,    doc,    score,   outlinks,  inlinks]
51. WT23-B38-87 0.0005138135064904477 1 1
52. WT27-B34-57 0.0005064132958536438 623 630

WT23-B38-87 is ranked higher than WT27-B34-57 but has far fewer inlinks. However, WT23-B38-87's one inlink is ranked #8. To compare, many of WT27-B34-57's inlinks are not even ranked in the top 500. This explains why the document with only one, highly relevant inlink is ranked higher than the document with many non-relevant inlinks.  
