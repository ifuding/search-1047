# ! /bin/bash

export proj=Search
export class=TermsSearch
export classSorter=SearchReSort
export dir=../tmp
export inputFileName=invertedIndex
export inputFile=${dir}/${inputFileName}
export pageRankName=PageRankMap
export urlLengthName=UrlVectorLength
export inputPageRank=${dir}/${pageRankName}
export inputUrlLen=${dir}/${urlLengthName}
export outputFile=${dir}/searchResult
export hadoopOut=part-r-00000

echo "Please enter the content you want to search: "
export pattern

read pattern


if [[ ! -f ${inputFile} ]]
then
  echo "Not found the input inverted file!"
  exit 1
fi

if [[ ! -d ${inputPageRank} ]]
then
  echo "Not found the input PageRank!"
  exit 1
fi

if [[ ! -d ${inputUrlLen} ]]
then
  echo "Not found the input UrlVecotorLength!"
  exit 1
fi


hadoop fs -mkdir /input
hadoop fs -rm /input/${inputFileName}
hadoop fs -put ${inputFile} /input
hadoop fs -rm -r /input/${pageRankName}
hadoop fs -put ${inputPageRank} /input
hadoop fs -rm -r /input/${urlLengthName}
hadoop fs -put ${inputUrlLen} /input
hadoop fs -rm -r /output
hadoop jar ../lib/${proj}.jar ${proj}Package.${class} /input/${inputFileName} /output ${pattern}

hadoop fs -rm /input/${hadoopOut}

hadoop fs -mv /output/${hadoopOut} /input
hadoop fs -rm -r /output
hadoop jar ../lib/${proj}.jar ${proj}Package.${classSorter} /input/${hadoopOut} /output

if [[ -f ${dir}/${hadoopOut} ]]
then
  rm ${dir}/${hadoopOut}
fi

hadoop fs -get /output/${hadoopOut} ${dir}/

if [[ -f ${outputFile} ]]
then
  rm ${outputFile}
else if [[ -d ${outputFile} ]]
then
  rm -r ${outputFile}
fi
fi

mv ${dir}/${hadoopOut} ${outputFile}

if [[ ! -f ${outputFile} ]]
then
  echo "Failure in searching the pattern!"
  exit 1
fi

