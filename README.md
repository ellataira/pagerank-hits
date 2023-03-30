[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-8d59dc4de5201274e310e4c54b9627a8934c3b88527886e3b421487c677d23eb.svg)](https://classroom.github.com/a/8Sq9xjZA)
# HW4

### Q: 

Explain in few sentences why some pages have a higher PageRank but a smaller inlink count. In particular for finding the explanation: pick such case pages and look at other pages that point to them.

### A: 

Some pages with low inlink counts still have higher PageRank scores than pages with more inlinks because they have inlinks of higher quality. If a document has an inlink with a very high PageRank score, then
it will have a higher updated score. In the PageRank algorithm, score has a positive relationship with its inlinks' scores; higher scored inlinks will lead to a higher scored document. 

For example, consider the following two documents, ranked 51 and 52: 

[rank,    doc,    score,   outlinks,  inlinks]
51. WT23-B38-87 0.0005138135064904477 1 1
52. WT27-B34-57 0.0005064132958536438 623 630

WT23-B38-87 is ranked higher than WT27-B34-57 but has far fewer inlinks. However, WT23-B38-87's one inlink is ranked #8. To compare, many of WT27-B34-57's inlinks are not even ranked in the top 500. This explains why the document with only one, highly relevant inlink is ranked higher than the document with many non-relevant inlinks.  