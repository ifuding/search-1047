#! /bin/bash
#计算PageRank值，利用awk动态判定是否达到收敛

export classname=Search
export dir=../tmp
PageRankOut=part-r-00000


hadoop fs -rm -r /input
hadoop fs -mkdir /input

#将url及其出链上传到HDFS
if [[ ! -f ${dir}/OutLinks ]]
then
  echo "Not found the input ${dir}/OutLinksMap."
  exit 1
else
  hadoop fs -put ${dir}/OutLinks /input
fi

#if [[ -d ${dir}/PageRankMap ]]
#then
  rm -ri ${dir}/PageRank*
  cp -a ${dir}/initPageRankMap ${dir}/PageRankMap
#fi

iterTime=0

while (($iterTime < 2))
do

((iterTime++))


hadoop fs -rm -r /input/PageRankMap
hadoop fs -put ${dir}/PageRankMap /input
hadoop fs -rm -r /output
hadoop jar "../lib/${classname}.jar"  "${classname}Package.PageRank" /input/OutLinks /output

if [[ -d ${PageRankOut} ]]
then
  rm -r ${PageRankOut}
fi

hadoop fs -get /output/${PageRankOut} ${dir}/


cmpRe=222

if [[ -f ${dir}/PageRank_old ]]
then
  java -cp ${CLASSPATH}:../lib/${classname}.jar ${classname}Package.MapFileRead ${dir}/${PageRankOut} | awk '/http/' > ${dir}/PageRank_new
  awk -f PageRankCmp ${dir}/PageRank_new ${dir}/PageRank_old
  cmpRe=$?
  mv ${dir}/PageRank_old ${dir}/PageRank_past
  mv ${dir}/PageRank_new ${dir}/PageRank_old
else
  java -cp ${CLASSPATH}:../lib/${classname}.jar ${classname}Package.MapFileRead ${dir}/${PageRankOut} | awk '/http/' > ${dir}/PageRank_old
fi

rm -r ${dir}/PageRankMap
mv ${dir}/${PageRankOut} ${dir}/PageRankMap

if ((cmpRe == 0))
then
  echo "PageRank completed. The itertime is $iterTime."   
fi


done
