package exKafka;

import kafka.utils.VerifiableProperties;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class DeviceStatusSerializer extends JacksonEncoder<DeviceStatus>
        implements Serializer<DeviceStatus>, Deserializer<DeviceStatus> {

    // Spark expects this constructor, unclear why, but it just must be here
    public DeviceStatusSerializer(final VerifiableProperties props) {
    }

    public DeviceStatusSerializer() {
    }

    @Override
    public DeviceStatus deserialize(final String topic, final byte[] data) {
        return fromBytes(data);
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // nothing to do
    }

    @Override
    public byte[] serialize(final String topic, final DeviceStatus data) {
        return toBytes(data);
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    protected Class<DeviceStatus> getSerializedType() {
        return DeviceStatus.class;
    }
}
