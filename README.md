search-1047
================================
基于Nutch和Hadoop的简易搜索引擎，排序的依据主要就是PageRank以及文本匹配值。

Nutch & Hadoop
----------------------------------
Nutch-1.9:http://www.apache.org/dyn/closer.cgi/nutch/. \<br /\>  
NUtch爬取产生的链接数据库（MapFile Format）linkdb，以及url的文本库segments/parse_text作为Hadoop的输入。\<br /\>  
Hadoop-2.6.0:http://www.apache.org/dyn/closer.cgi/hadoop/common/.\<br /\>  


文本预处理
----------------------------------
作为PageRank的输入\<br /\>  
1. SequenceFileRead.java: 将linkdb/data(SequenceFile Format)转换成linkdb_data(Text Format).\<br /\>  
2. OutLinks.java(MapReduce): 将linkdb_data转换成OutLinks_db(Text Format:\<br /\>   每行的第一个字段为源url，后面的所有字段为第一个url的出链）。\<br /\>  

PageRank
----------------------------------
1. Input: OutLinks_db(Text Format), PageRankMap(MapFile Format)\<br /\>  
   Output: newPageRankMap.\<br /\>  
2. Mapper： 将\<null, src_url and outlink urls\>转换成\<url from outlink urls, pageRank[src_url]/outLinkNum[src_url]\>\<br /\>  
Reduccer：将\<url, Iterable\<pageRank_part\>\>转换成\<url, 1-dampFactor+damFactor*sum(pageRank_part)\>.\<br /\>  
3. to be optimized.\<br /\>  
(1) mapreduce is slow for reading the MapFile.\<br /\>  
(2) programming to settle the number of iterations of the PageRank.\<br /\>  

Search
----------------------------------
读取PageRank，同时计算网页的文本匹配值，将两个值进行加权由hadoop排序。\<br /\>  
