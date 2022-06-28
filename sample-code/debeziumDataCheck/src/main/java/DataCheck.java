

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.pulsar.client.api.*;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;

import java.util.regex.Pattern;

public class DataCheck {
    public static String getStrFromDate(Date signInDate) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(signInDate);
    }

    public static void main(String[] args) throws PulsarClientException {
//
//        String serviceUrl=args[0]; //"pulsar://20.228.217.226:6650"
//        String topic = args[1];
//        String subscription = args[2];

        String serviceUrl="pulsar://20.228.217.226:6650";
        String topic = "cdc/cdc-mysql/mysqlserver1.test.debezium_testcc1";
        String subscription = "test-sub1";

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .connectionTimeout(30000, TimeUnit.MILLISECONDS)
                //.listenerName("allNet")
                .build();
        // 构造生产者
            Pattern pattern = Pattern.compile("cdc/cdc-mysql/mysqlserver1.test.debezium_testcc.*");
            System.out.println(pattern.toString());

        Consumer<byte[]> consumer = client.newConsumer()
//                .topic(topic)
                .topicsPattern(pattern)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        int lastCId = -1;
        int lastUId = -1;
        int lastDId = -1;

        int CCount = 0;
        int UCount = 0;
        int DCount = 0;

        while(true) {
            Message<byte[]> msg = consumer.receive();
            MessageId messageId = msg.getMessageId();
            String value = new String (msg.getValue());

            JSONObject json = JSON.parseObject(value);
            String op = json.getString("op");
            JSONObject beforeJson = json.getJSONObject("before");
            JSONObject afterJson = json.getJSONObject("after");

            if(op.equals("c")) { //DML中insert对应的op为c，update对应为u，delete对应为d。
                //如果用topicPatten方式，同时sub 多个topic，op：c 操作可能是不同 topic里数据，但是我们关心的是同一个topic里的顺序。
                //insert have no beforeJson
                CCount++;
                int id = afterJson.getIntValue("id");
                //System.out.println("bengin process op c ,first id is:" + id);
                if (lastCId == -1) {
                    lastCId = id;
                } else {
                    if (id < lastCId) {
                        System.out.println("encounter disorder in op: " + op + " in id : " + id+ " prepare exit");
                        System.exit(1);
                    }
                    lastCId = id;
                }

                // calculate time
                long tableUpdateTime = afterJson.getLong("create_time");
                long connectorProcessTime = json.getLong("ts_ms");
                long nowTime = System.currentTimeMillis();
                //System.out.println(" tableUpdateTime " + tableUpdateTime + " connectorProcessTime " + connectorProcessTime);
                //System.out.println(" tableUpdateTime " + tableUpdateTime + " from table to connector : " + (connectorProcessTime-tableUpdateTime));
                System.out.println(getStrFromDate(new Date())+ " CCount：" + CCount + " from mysql to connector take ms： " + (connectorProcessTime-tableUpdateTime) + " from connector to consume take ms: " + (nowTime - connectorProcessTime)+" from mysql to now consume takes ms: "+(nowTime-tableUpdateTime) + " message id :" + messageId );

            } else if(op.equals("u")) { //DML中insert对应的op为c，update对应为u，delete对应为d。
                // update include beforeJson and afterJson
                UCount++;
                int id = afterJson.getIntValue("id");
                //System.out.println("begin process op u ,first id is:" + id);
                if(lastUId == -1) {
                    lastUId = id;
                } else {
                    if(id < lastUId) {
                        System.out.println("encounter disorder in op: " + op + " in id : " + id +" prepare exit ");
                        //System.out.println("error:" + op + ",id:" + beforeJson.get("id") + ",value:" + beforeJson + ",messageId:" + messageId);
                        System.exit(1);
                    }
                    lastUId = id;
                }
                // calculate time
                long tableUpdateTime = afterJson.getLong("create_time");
                long connectorProcessTime = json.getLong("ts_ms");
                long nowTime = System.currentTimeMillis();
                //System.out.println(" tableUpdateTime " + tableUpdateTime + " connectorProcessTime " + connectorProcessTime);
                //System.out.println(" tableUpdateTime " + tableUpdateTime + " from table to connector : " + (connectorProcessTime-tableUpdateTime));

                System.out.println(getStrFromDate(new Date())+ " UCount: "+ UCount +" from mysql to connector take ms： " + (connectorProcessTime-tableUpdateTime) + " from connector to consume take ms: " + (nowTime - connectorProcessTime) + " from mysql to now consume takes ms: "+(nowTime-tableUpdateTime) + " message id :" + messageId);

            } else if(op.equals("d")) { //DML中insert对应的op为c，update对应为u，delete对应为d。
                // delete can not get tableUpdateTime
                DCount ++;
                int id = beforeJson.getIntValue("id");
                //System.out.println("bengin process op d ,first id is:" + id);
                if (lastDId == -1) {
                    lastDId = id;
                } else {
                    if (id < lastDId) {
                        System.out.println("encounter disorder in op: " + op + " in id : " + id + " prepare exit ");
                        //System.out.println("error:" + op + ",id:" + beforeJson.get("id") + ",value:" + beforeJson + ",messageId:" + messageId);
                        System.exit(1);
                    }
                    lastDId = id;
                }
                //System.out.println(op + ",id:" + beforeJson.get("id") + ",value:" + beforeJson + ",messageId:" + messageId);
                //System.out.println("delete order is ok");
                long connectorProcessTime = json.getLong("ts_ms");
                long nowTime = System.currentTimeMillis();
                //System.out.println(" tableUpdateTime " + tableUpdateTime + " connectorProcessTime " + connectorProcessTime);
                //System.out.println(" tableUpdateTime " + tableUpdateTime + " from table to connector : " + (connectorProcessTime-tableUpdateTime));
                System.out.println(getStrFromDate(new Date()) + " DCount: "+ DCount +" from mysql to connector take ms：no " + " from connector to consume take ms: " + (nowTime - connectorProcessTime) + " message id :" + messageId);


            } else {
                System.out.println(" not insert,update,delete op " + messageId + "，" + value);
            }
            consumer.acknowledgeAsync(msg);
        }
    }
}
