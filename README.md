# Hadoop_information_Retrieval_Metrics

Information retrieval (IR) is concerned with finding material (e.g., documents) of an
unstructured nature (usually text) in response to an information need (e.g., a query) from
large collections. One approach to identify relevant documents is to compute scores
based on the matches between terms in the query and terms in the documents. For
example, a document with words such as ball, team, score, championship is likely to be
about sports. It is helpful to define a weight for each term in a document that can be
meaningful for computing such a score. We describe below popular information retrieval
metrics such as term frequency, inverse document frequency, and their product, term
frequency-inverse document frequency (TF-IDF), that are used to define weights for
terms.
Term Frequency:
Term frequency is the number of times a particular word t occurs in a document d.
TF(t, d) = No. of times t appears in document d
Since the importance of a word in a document does not necessarily scale linearly with the
frequency of its appearance, a common modification is to instead use the logarithm of the
raw term frequency.
WF(t,d) = 1 + log10(TF(t,d)) if TF(t,d) > 0, and 0 otherwise
We will use this logarithmically scaled term frequency in what follows.
Inverse Document Frequency:
The inverse document frequency (IDF) is a measure of how common or rare a term is
across all documents in the collection. It is the logarithmically scaled fraction of the
documents that contain the word, and is obtained by taking the logarithm of the ratio of
the total number of documents to the number of documents containing the term.
IDF(t) = log10 (Total number of documents / Num. of documents containing term t).
TF-IDF:
Term frequency–inverse document frequency (TF-IDF) is a numerical statistic that is
intended to reflect how important a word is to a document in a collection or corpus of
documents. It is often used as a weighting factor in information retrieval and text mining.
TF-IDF(t, d) = WF(t,d) * IDF(t)
