package exKafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Random;

import static exKafka.KafkaIntegrationUtils.getProducerConfig;

public class MessageProducer {

    public static final String TOPIC = "device-status";

    public static void main(String[] args) {
        final Random random = new Random(123);

        Producer<String, DeviceStatus> producer = null;
        try {
            producer = new KafkaProducer<>(getProducerConfig(), new StringSerializer(), new DeviceStatusSerializer());
            while (true) {
                Thread.sleep(1000);
                producer.send(new ProducerRecord<String, DeviceStatus>(
                        TOPIC,
                        new DeviceStatus()
                                .withTimestamp(System.currentTimeMillis())
                                .withDevice("device_name")
                                .withValue(random.nextDouble() * 100)));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (producer != null) {
                producer.close();
            }
        }

    }


}
