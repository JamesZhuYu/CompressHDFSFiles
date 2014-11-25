o_e() {
  echo "$@" 1>&2
}

err() {
  echo_e "$0: $@"
  usage
}

usage() {
  echo_e "Usage: $0 [options] src_dir merged_dir dest_dir [ hadoop_dir ]"
  echo_e 'Avalaible [options] are: '
  echo_e "  -a <action name>"
  echo_e "  -c <bash configuration script>"
  echo_e "  -t compress hdfs files days #2014-10-28"
  echo_e "  -d <log filename domain> # 'POPPEN.' is default"
  echo_e "  -h this help"
  echo_e "  -l <log file path to append processing results> # /dev/stderr and /dev/stdout is default, only work when -c is specified"
  echo_e "  -p hadoop program path"
  echo_e "either -a or -c must be specified, if -c is specified, other option is ignored except -l or -h"
  exit 1
}

DOMAIN=POPPEN
HADOOP=/usr/local/tncdata/hadoop/bin/hadoop
#JAR=/usr/local/tncdata/jobs/tda-mapred-0.1.0-full-compress.jar
JAR=/usr/local/tncdata/tmp/james/tnc/Sprint_48/tda-mapred-0.1.0-full-compress.jar
while getopts ":d:a:c:l:t:hp:" opt
do
  case $opt in
     a)  ACTION="$OPTARG";;
     c)  CONFIG="$OPTARG";;
     d)  DOMAIN="$OPTARG";;
     l)  DATA_MERGE_LOG="$OPTARG";;
     t)  DAY="$OPTARG";;
     p)  HADOOP="$OPTARG";;
     h)  usage;;
     :)  err "option -$OPTARG requires a parameter";;
     \?) err "unknown option: -$OPTARG";;
     # *) echo "$opt: $OPTARG";;
  esac
done

echo "$(date '+%Y-%m-%d %H:%M:%S %:z'): Begin to Compress $DAY $DOMAIN HDFS Files"
if [ -n "$CONFIG" ]; then
  if [ -f "$CONFIG" ]; then
    echo
    echo "$(date '+%Y-%m-%d %H:%M:%S %:z'): load config '$CONFIG' ..."
    . "$CONFIG"
    for action in "${ACTIONS[@]}"; do
      PRE="$HADOOP_LOG_DIR/$DOMAIN/userlog/$action/$(echo $DAY | tr - /)/${DOMAIN}.${action}_${DAY}"
      s_dir=$PRE.log
      bz_dir=$PRE.bz2

      #hadoop jar tda-mapred-0.1.0-full.jar com.thenetcircle.tda.job.CompressHDFSFile compress hdfs://cloud-host-02:9000/src.log hdfs://cloud-host-02:9000/dest.bz2
      #hadoop jar tda-mapred-0.1.0-full.jar com.thenetcircle.tda.job.CompressHDFSFile uncompress hdfs://cloud-host-02:9000/dest.bz2 hdfs://cloud-host-02:9000/tmp.log
      if hadoop fs -test -e $s_dir;then
        echo "$(date '+%Y-%m-%d %H:%M:%S %:z'): job $action $DAY started "
        for (( c=0; c < 6; ++c )); do
          if hadoop jar $JAR com.thenetcircle.tda.job.CompressHDFSFile compress $s_dir $bz_dir; then
              echo "$(date '+%Y-%m-%d %H:%M:%S %:z'): job $action $DAY successed "
              echo "$(date '+%Y-%m-%d %H:%M:%S %:z'): hadoop fs -rmr $s_dir "
              hadoop fs -rmr $s_dir
              break;
          else
              if [ "$c" != "5" ]; then
                echo "$(date '+%Y-%m-%d %H:%M:%S %:z'): $action $DAY job compress failed, sleep 10 seconds, then will retry No. $c: ";
                sleep 10;
              else
                echo_e "$action $DAY job failed $(date '+%Y-%m-%d %H:%M:%S %:z') ";
                echo_e " hadoop fs -rmr $bz_dir "          
                hadoop fs -rmr $bz_dir
              fi
          fi
        done
      fi
    done
  else
    err "configuration file $CONFIG does not exist"
  fi
fi
echo "$(date '+%Y-%m-%d %H:%M:%S %:z'): Compress $DAY $DOMAIN HDFS Files Finished"
