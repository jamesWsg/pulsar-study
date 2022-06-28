#LOG=
TABLE_PREFIX=debezium_testee
PULSAR_URL=pulsar://168.64.5.1:6650
TOPIC_PREFIX=cdc/cdc-mysql
DATABASE_NAME=test


for i in {1..100}
do

# jar has three parameter: 1,service url; 2,topic ;3,subscription name
nohup java -jar debeziumDataCheckHuaitai-1.0-SNAPSHOT.jar ${PULSAR_URL} ${TOPIC_PREFIX}/mysqlserver1.${DATABASE_NAME}.${TABLE_PREFIX}${i} sub-dataCheck > ${TABLE_PREFIX}${i}.log&
echo ${i}

done
