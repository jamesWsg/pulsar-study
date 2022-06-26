import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import java.util.concurrent.TimeUnit;

public class ProduceTimeInfo {
    public static void main(String[] args) throws Exception {
//        String serviceUrl="pulsar://20.228.217.226:6650";
        String serviceUrl="pulsar://localhost:6650";
        String topic = "public/default/test";
        String subscription = "test-schema";

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .connectionTimeout(30000, TimeUnit.MILLISECONDS)
//                .listenerName("allNet")
                .build();
        
        Consumer<TimeInfo> consumer = client.newConsumer(JSONSchema.of(TimeInfo.class)).topic(topic)
                .subscriptionName("sub-schema").subscribe();
        Producer<TimeInfo> producer = client.newProducer(JSONSchema.of(TimeInfo.class))
                .topic(topic)
                .create();

        int numMessages = 100;
        for (long i = 0; i < numMessages; i++) {
            final String orderId = "id-" + i;
            long nowTime = System.currentTimeMillis();
            TimeInfo TimeInfo = new TimeInfo(orderId,nowTime);
            // send the payment in an async way
            producer.newMessage()
                    .key(orderId)
                    .value(TimeInfo)
                    .sendAsync();
        }

        // flush out all outstanding messages
        producer.flush();

        System.out.printf("Successfully produced %d messages to a topic called %s%n",
                numMessages, topic);
    }

}
