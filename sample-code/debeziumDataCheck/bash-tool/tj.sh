UPDATE_LOG=update.tj
INSERT_LOG=insert.tj

for i in `ls *.log`; do
  echo 'do '$i >> $INSERT_LOG
  echo 'do '$i >> $UPDATE_LOG

cat $i |awk '{sum+=$25} END {print "CCount,update Average latency = ", sum/NR}' >> $INSERT_LOG
cat $i |awk '{sum+=$26} END {print "UCount,update Average latency = ", sum/NR}' >> $UPDATE_LOG

done
