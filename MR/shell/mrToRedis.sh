#!/bin/bash
#Created by lsj 2017-2-15

if [ $# -lt 1 ]; then
        echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<CITYCODE> "
        exit 1
fi

CITYCODE=$1

SHELLPATH=$(cd `dirname $0`; pwd)
echo $SHELLPATH
[ -z $EXAMPLE_HOME ] && EXAMPLE_HOME=`cd "$SHELLPATH/../" >/dev/null; pwd`
echo $EXAMPLE_HOME
WORKPATH=$EXAMPLE_HOME

DATE=`date +"%Y%m%d"`

JAVALIB=${WORKPATH}/mr
LOGFILE=${WORKPATH}/log/dmp-mrToRedis_${CITYCODE}_${DATE}.log
MRJAR=$JAVALIB/mrToRedis.jar

#运行参数，input有多个时，用逗号分隔如 -D input=/tmp/wordcount/input,/tmp/wordcount/input2
ARGS="
-D input=/user/hive/warehouse/dmp.db/dm_userlabel_http/${CITYCODE} 
-D cityCode=${CITYCODE} 
-conf ${WORKPATH}/conf/redis-config.xml 
"

echo "ARGS" ${ARGS} 
echo "WORKPATH" ${WORKPATH}
echo "LOGFILE" ${LOGFILE}
echo "MRJAR" ${MRJAR}

echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE

hadoop jar ${MRJAR} org.aotain.dmp.RedisDriver $ARGS  1>>$LOGFILE 2>>$LOGFILE

ret=$?
if [ ${ret} -ne 0 ]; then
     exit ${ret}
fi

echo `date +"%Y-%m-%d %H:%M:%S"`    "Exec Success [$DATE]!" | tee -a $LOGFILE

exit 0
