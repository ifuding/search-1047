# ! /bin/bash

export proj=Search
export class=UrlModulus
export dir=../tmp
export inputFileName=invertedIndex
export outputFileName=UrlVectorLength
export inputFile=${dir}/${inputFileName}
export outputFile=${dir}/${outputFileName}
export outputTxt=${dir}/UrlLength
export hadoopOut=part-r-00000

if [[ ! -f ${inputFile} ]]
then
  echo "Not found the inputFile!"
  exit 1
fi

hadoop fs -mkdir /input
hadoop fs -rm /input/${inputFileName}
hadoop fs -put ${inputFile} /input
hadoop fs -rm -r /output
hadoop jar ../lib/${proj}.jar ${proj}Package.${class} /input/${inputFileName} /output

if [[ -d ${dir}/${hadoopOut} ]]
then
  rm -r ${dir}/${hadoopOut}
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

if [[ ! -d ${outputFile} ]]
then
  echo "Failure in generating the inverted file!"
  exit 1
fi

if [[ -f ${outputTxt} ]]
then
  rm ${outputTxt}
fi

java -cp ${CLASSPATH}:../lib/${proj}.jar ${proj}Package.SequenceFileRead ${outputFile}/data > ${outputTxt}

