import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.api.Message;
import java.util.concurrent.TimeUnit;

public class ConsumeTimeInfo {
    public static void main(String[] args) throws Exception {
//        String serviceUrl="pulsar://20.228.217.226:6650";
        String serviceUrl="pulsar://localhost:6650";
        String topic = "public/default/test";
        String subName = "test-schema";

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .connectionTimeout(30000, TimeUnit.MILLISECONDS)
//                .listenerName("allNet")
                .build();

        Consumer<TimeInfo> consumer = client.newConsumer(JSONSchema.of(TimeInfo.class)).topic(topic)
                .subscriptionName(subName).subscribe();

        while (true) {
            Message<TimeInfo> msg = consumer.receive();

            final String key = msg.getKey();
            final TimeInfo timeinfo = msg.getValue();
            long messageTime= timeinfo.getCreateTime();
            long nowTime = System.currentTimeMillis();
            long latency = nowTime - messageTime;


            System.out.printf("key = %s, value = %s%n, latency = %d%n", key, timeinfo,latency);
        }


    }
}
