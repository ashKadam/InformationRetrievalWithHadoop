# InformationRetrievalWithHadoop
##Information Retrieval using Map-Reduce technique in Hadoop

Information retrieval (IR) is concerned with finding material (e.g., documents) of an
unstructured nature (usually text) in response to an information need (e.g., a query) from
large collections. One approach to identify relevant documents is to compute scores
based on the matches between terms in the query and terms in the documents. For
example, a document with word s such as bal l, team, score, c hampionship is likely to be
about sports. It is helpful to define a weight for each term in a document that can be
meaningful for computing such a score. We describe below popular information retrieval
metrics such as term frequency, inverse document frequency, and their product, term
frequency-inverse document frequency (TF-IDF), that are used to define weights for
terms.


**Term Frequency:**
Term frequency is the number of time s a particular word t occ urs in a document d.

  **TF(t, d) = No. of times t appears in document d**
  
Since the importance of a word in a document does not necessarily scale linearly with the
frequency of its appearance, a common modification is to instead use the logarithm of the
raw term frequency.

  **WF(t,d) = 1 + log 10 (TF(t,d)) if TF(t,d) > 0, and 0 otherwise**   (equation#1)
  
We will use this logarithmically scaled term frequency in what follows.


**Inverse Document Frequency:**
The inverse document frequency (IDF) is a measure of how common or rare a term is
across all documents in the collection. It is the logarithmically scaled fraction of the
documents that contain the word, and is obtained by taking the logarithm of the ratio of
the total number of documents to the number of documents containing the term.

  **IDF(t) = log 10 (Total # of documents / # of documents containing term t)**   (equation#2)
  
Under this IDF formula, terms appearing in all documents are assumed to be stopwords and
subsequently assigned IDF=0. We will use the smoothed version of this formula as follows:
  
  **IDF(t) = log 10 (1 + Total # of documents / # of documents containing term t)**   (equation#3)
  
Practically, smoothed IDF helps alleviating the out of vocabulary problem (OOV), where it is
better to return to the user results rather than nothing even if his query matches every single
document in the collection.


**TF-IDF:**
Term frequency–inverse document frequency (TF-IDF) is a numerical statistic that is
intended to reflect how important a word is to a document in a collection or corpus of
documents. It is often used as a weighting factor in information retrieval and text mining.
  
  **TF-IDF(t, d) = WF(t,d) * IDF(t)**  (equation#4)
  
  
  
  **Following are the instructions to execute the programs.**

Here onwards please consider below notations:
* input_path -> /user/cloudera/input ------- input folder has all 8 input files
* output_path -> /user/cloudera/output ----- final result will be written in output folder
* intermediate_path -> /user/cloudera/intern_file  ----- intermediate results from termFrequency to TFIDF
* tfInter_path -> /user/cloudera/tfInter ----- first intermediate results (input for TFIDF) while searching
* idfInter_path -> /user/cloudera/idfInter ----- next intermediate results (input for Search) while searching

Note: Instructions are written in the context of HDFS. All input and output files are stored on HDFS. 
      While running any program again, please clear the output path (delete all earlier intermediate and output files)

1. DocWordCount.java
   This file takes input files and list down all words with their count as output.
   Compile and create jar file. Run program using this jar and passing input and output path. 
   Then store results in required output file.
   
   	 javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java -d build -Xlint
     
   	 jar -cvf docwordcount.jar -C build/ .
     
   	 hadoop jar docwordcount.jar org.myorg.DocWordCount <input_path> <output_path>
     
   	 hadoop fs -cat <output_path> > DocWordCount.out
     
   
2. TermFrequency.java

   This file takes input files and list down all words with their term frequency and file name as output.
   Compile and create jar file. Run program using this jar and passing input and output path. 
   Then store results in required output file.    
   
   	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrequency.java -d build -Xlint
    
   	jar -cvf termfrequency.jar -C build/ .
    
   	hadoop jar termfrequency.jar org.myorg.TermFrequency <input_path> <output_path>
    
   	hadoop fs -cat <output_path> > TermFrequency.out
    

3. TFIDF.java

   This file takes input files and list down all words with their tfidf values and their file names as output.
   We are using chaing here as we arre using output of term frequency as input for TFIDF. 
   Compile and create jar file. Run program using this jar and passing input and output path. 
   Then store results in required output file.    
   
   	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrequency.java TFIDF.java -d build -Xlint
    
   	jar -cvf tfidf.jar -C build/ .
    
   	hadoop jar tfidf.jar org.myorg.TFIDF <input_path> <inmtermediate_file> <output_path>
    
   	hadoop fs -cat <output_path> > TFIDF.out
    

4. Search.java

   This file takes input files and list down file with thier TFIDF for matching query word/token. 
   Compile and create jar file. Run program using this jar and passing input and output path. 
   Then store results in required output file.    
   
   	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Search.java -d build -Xlint
    
   	jar -cvf search.jar -C build/ .
    
   	hadoop jar search.jar org.myorg.Search <input_path> <output_path> <user_query>
    
   	hadoop fs -cat <output_path> > query1.out
    

*** Before running second query, please delete /user/clouder/output, /user/cloudera/intermediate1 and /user/cloudera/searchintermediate1.

5. Rank.java

   This file takes input files and list down all files with dreaseing TFIDF values. 
   Compile and create jar file. Run program using this jar and passing input and output path. 
   Then store results in required output file.    
   
   	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Rank.java -d build -Xlint
    
   	jar -cvf rank.jar -C build/ .
    
   	hadoop jar rank.jar org.myorg.Rank <input_path> <output_path> <user_query>
    
   	hadoop fs -cat <output_path> > query1-rank.out
    


Additionally, we have Search_withchaining. java and corresponding output. Similar command structure is used 
as that of TFIDF. 

    javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrequency.java TFIDF.java SearchWithchaining.java -d build -Xlint
    
  	 jar -cvf searchwithchaining.jar -C build/ .
    
   	hadoop jar searchwithchaining.jar org.myorg.SearchWithChaining <input_path> <tfIntern path> <idfIntern path><output_path> <user_query>
    
   	hadoop fs -cat <output_path> > query1-chaining.out
    


