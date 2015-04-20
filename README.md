# search-1047
A simple search engine based on Nutch and Hadoop.

# Nutch爬虫
Nutch-1.9:http://www.apache.org/dyn/closer.cgi/nutch/.
NUtch爬取产生的链接数据库（MapFile Format）linkdb，以及url的文本库segments/parse_text作为Hadoop的输入。

# Hadoop Mapreduce
Hadoop-2.6.0:http://www.apache.org/dyn/closer.cgi/hadoop/common/.
search排序的依据主要就是PageRank以及文本匹配值。

# OutLinks以及OutLinkNum
1. SequenceFileRead.java: 将linkdb/data(SequenceFile Format)转换成linkdb_data(Text Format).
2. OutLinks.java(MapReduce): 将linkdb_data转换成OutLinks_db(Text Format: 每行的第一个字段为源url，后面的所有字段未此url的出链）。

# PageRank(important and need to be optimized)
1. Input: OutLinks_db(Text Format), PageRankMap(MapFile Format)
Output: newPageRankMap.
2. Mapper： 将\<null, src_url and outlink urls\>转换成\<url from outlink urls, pageRank[src_url]/outLinkNum[src_url]\>
Reduccer：将\<url, Iterable\<pageRank_part\>\>转换成\<url, 1-dampFactor+damFactor*sum(pageRank_part)\>.
3. to be optimized.
(1) mapreduce is slow for reading the MapFile.
(2) programming to settle the number of iterations of the PageRank.

# Search(to be optimized)
读取PageRank，同时计算网页的文本匹配值，将两个值进行加权由hadoop排序。
