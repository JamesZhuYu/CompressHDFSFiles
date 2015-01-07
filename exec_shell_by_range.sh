#!/bin/bash
#/compressHDFSFiles.sh -c /usr/local/tncdata/tool/cronjob/data_log_merge_config.sh -t 2013-06-07 >> /var/log/datacron/tnc_data_log_compress.log 2>> /var/log/datacron/tnc_data_log_compress.error
script="/usr/local/tncdata/tool/cronjob/compressHDFSFiles.sh"
file="$1"
start_ts="$2";
end_ts="$3";
if ! echo $start_ts | grep -qE '^[0-9]+$'; then
  start_ts=$(date -d "$start_ts 23:59:59" +%s);
fi
if ! echo $end_ts | grep -qE '^[0-9]+$'; then
  end_ts=$(date -d "$end_ts 23:59:59" +%s);
fi
for ((i=$start_ts; i >= $end_ts; i -= 86400)); do
  d=$(date -d @$i +%Y-%m-%d)
  sh $script -c $file -t $d >> /var/log/datacron/tnc_data_log_compress.log 2>> /var/log/datacron/tnc_data_log_compress.error
done