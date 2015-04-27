#! /bin/bash
#读取用户输入，运行搜索程序

export classname=Search
export searchPattern
export dir=../tmp
export searchOut=part-r-00000
echo "Please enter the content you want to search: "
read searchPattern
echo ${searchPattern}
exit 0 

hadoop fs -rm /input
hadoop fs -mkdir /input
hadoop fs -rm -r /output
hadoop fs -put ${dir}/PageRankMap /input
hadoop fs -put ${dir}/parse_text /input
hadoop jar ../lib/${classname}.jar ${classname}Package.${classname}  /input/parse_text  /output ${searchPattern}

if [[ -f ${dir}/${searchOut} ]]
then
  rm  ${dir}/${searchOut}
else
  rm -r ${dir}/${searchOut}
fi

hadoop fs -get /output/${searchOut} ${dir}/

tail -n -100 ${dir}/${searchOut} > ${dir}/searchRe
