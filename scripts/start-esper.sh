#!/usr/bin/env bash

export _JAVA_OPTIONS="-Xmx1g"

bootstrap_opt=""
topic_opt=""
exp_opt=""
maxevents_opt=""
query_opt=""
rate_opt=""

bootstrap=""
topic=""
exp=""
maxevents=""
query=""
rate=""

while getopts "b:t:e:m:q:r:" arg;
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
      rate_opt="--query"
      echo $rate;;
    * )
      echo "$arg is an invalid flag";;
  esac
done



# Execute producer
echo "Start loading:"
echo ${bootstrap_opt} ${bootstrap} ${topic_opt} ${topic} ${exp_opt} ${exp} ${maxevents_opt} ${maxevents} ${query_opt} ${query} ${rate_opt} ${rate}
java -cp $PROJECT_DIR/target/ICEP-esper-1.0-SNAPSHOT-jar-with-dependencies.jar esper.KafkaAdaptedEsper ${bootstrap_opt} ${bootstrap} ${topic_opt} ${topic} ${exp_opt} ${exp} ${maxevents_opt} ${maxevents} ${query_opt} ${query} ${rate_opt} ${rate} &
echo "Producer finished"
