#!/usr/bin/env bash

#export _JAVA_OPTIONS="-Xmx1g"

if [ -z "$PROJECT_DIR" ]
then
      PROJECT_DIR="/home/hadoop/Samuele/ICEP-esper"
else
      echo "PROJECT_DIR is $PROJECT_DIR"
fi

bootstrap_opt=""
topic_opt=""
exp_opt=""
maxevents_opt=""
query_opt=""
rate_opt=""
duration_opt=""
file_opt=""

bootstrap=""
topic=""
exp=""
maxevents=""
query=""
rate=""
duration=""
file=""

while getopts "b:t:e:m:q:r:d:f:" arg;
do
  case ${arg} in
    b )
      bootstrap=$OPTARG
      bootstrap_opt="--bootstrap"
      echo $bootstrap;;
    t )
      topic=$OPTARG
      topic_opt="--topic"
      echo $topic;;
    e )
      exp=$OPTARG
      exp_opt="--exp"
      echo $exp;;
    m )
      maxevents="$OPTARG"
      maxevents_opt="--maxevents"
      echo $maxevents;;
    q )
      query=$OPTARG
      query_opt="--query"
      echo $query;;
    r )
      rate=$OPTARG
      rate_opt="--rate"
      echo $rate;;
    d )
      duration=$OPTARG
      duration_opt="--duration"
      echo $rate;;
    f )
      file=$OPTARG
      file_opt="--file"
      echo $rate;;
    * )
      echo "$arg is an invalid flag";;
  esac
done



# Execute producer
echo "Start loading:"
echo ${bootstrap_opt} ${bootstrap} ${topic_opt} ${topic} ${exp_opt} ${exp} ${maxevents_opt} ${maxevents} ${query_opt} ${query} ${rate_opt} ${rate}
java -Xmx20g -cp $PROJECT_DIR/target/ICEP-ee.ut.cs.esper-1.0-SNAPSHOT-jar-with-dependencies.jar ee.ut.cs.esper.KafkaAdaptedEsper ${bootstrap_opt} ${bootstrap} ${topic_opt} ${topic} ${exp_opt} ${exp} ${maxevents_opt} ${maxevents} ${query_opt} ${query} ${rate_opt} ${rate} ${duration_opt} ${duration} ${file_opt} ${file}&> esper.out &
echo "Producer finished"
