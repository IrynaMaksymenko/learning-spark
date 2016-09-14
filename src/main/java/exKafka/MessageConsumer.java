package exKafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.DefaultDecoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static exKafka.KafkaIntegrationUtils.getConsumerConfig;
import static exKafka.MessageProducer.TOPIC;
import static java.lang.String.format;

public class MessageConsumer {

    public static void main(String[] args) {

        final ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(getConsumerConfig());
        final Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(TOPIC, 1);
        final Map<String, List<KafkaStream<byte[], DeviceStatus>>> consumerMap =
                consumer.createMessageStreams(topicCountMap, new DefaultDecoder(null), new DeviceStatusSerializer());

        final List<KafkaStream<byte[], DeviceStatus>> streams = consumerMap.get(TOPIC);
        for (KafkaStream<byte[], DeviceStatus> stream : streams) {
            final ConsumerIterator<byte[], DeviceStatus> streamIterator = stream.iterator();
            try {
                while (streamIterator.hasNext()) {
                    final DeviceStatus deviceStatus = streamIterator.next().message();
                    System.out.println(format("DeviceStatus = (%s, %s, %s)",
                            deviceStatus.getTimestamp(), deviceStatus.getDevice(), deviceStatus.getValue()));
                }
            } finally {
                consumer.shutdown();
            }
        }

/*
        // next code is valid for 0.9 kafka api, but not for 0.8

        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("partition.assignment.strategy", "range");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", DeviceStatusSerializer.class.getName());
        Consumer<String, DeviceStatus> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(TOPIC);
        while (true) {
            Map<String, ConsumerRecords<String, DeviceStatus>> recordsByTopic = consumer.poll(100);
            final ConsumerRecords<String, DeviceStatus> records = recordsByTopic.get(TOPIC);
            if (records != null) {
                for (ConsumerRecord<String, DeviceStatus> record : records.records())
                    try {
                        System.out.println(format("offset = %d, DeviceStatus = (%s, %s, %s)", record.offset(),
                                record.value().getTimestamp(), record.value().getDevice(), record.value().getValue()));
                    } catch (Exception e) {
                        System.out.println("Could not process consumed record");
                    }
            }
        }
*/
    }

}
