# ! /bin/bash

export proj=Search
export dir=../tmp
export parseName=parse_text
export parseText=${dir}/${parseName}
export hadoopOut=part-r-00000

if [[ ! -f ${parseText} ]]
then
  echo "Not found the parse_text!"
  exit 1
fi

hadoop fs -mkdir /input
hadoop fs -put ${parseText} /input
hadoop fs -rm -r /output
hadoop jar ../lib/${proj}.jar ${proj}Package.TermsSelector /input/${parseName} /output

if [[ -f ${dir}/${hadoopOut} ]]
then
  rm ${dir}/${hadoopOut}
fi

hadoop fs -get /output/${hadoopOut} ${dir}/
if [[ -f ${dir}/invertedIndex ]]
then
  rm -r ${dir}/invertedIndex
else if [[ -d ${dir}/invertedIndex ]]
then
  rm -r ${dir}/invertedIndex
fi
fi

mv ${dir}/${hadoopOut} ${dir}/invertedIndex

if [[ ! -f ${dir}/invertedIndex ]]
then
  echo "Failure in generating the inverted file!"
  exit 1
fi

./UrlModulus.sh
