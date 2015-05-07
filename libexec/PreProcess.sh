# ! /bin/bash
#

if ! ./OutLinks.sh
then
  echo "Failure in generating the OutLinks!"
  exit 1
fi 

if ! ./PageRank.sh
then
  echo "Failure in calculating the PageRank!"
  exit 1
fi
