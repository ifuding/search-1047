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
1. input: OutLinks_db(Text Format), OutLinkMap(MapFileFormat), PageRankMap(MapFile Format)
output: newPageRankMap.
2. Mapper: \<null, src_url and outlink urls\> --> \<url from outlink urls, pageRank[src_url]/outLinkNum[src_url]\>
Reduccer: \<url, Iterable<pageRank_part>\> --> \<url, 1-dampFactor+damFactor*sum(pageRank_part)\>.
3. to be optimized.
(1) mapreduce is slow for reading the two MapFiles.
(2) programming to settle the number of iterations of the PageRank.

# Search(to be optimized)
combine the text matchVal and the pageRank to sort th urls.
