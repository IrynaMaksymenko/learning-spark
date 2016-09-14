package exKafka;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;

import java.io.IOException;
import java.math.BigDecimal;

import static java.lang.String.format;

public abstract class JacksonEncoder<T> implements Encoder<T>, Decoder<T> {

    private static final JsonSerializer<Double> DOUBLE_SERIALIZER = new JsonSerializer<Double>() {
        @Override
        public void serialize(final Double aDouble, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeNumber(
                    BigDecimal.valueOf(aDouble)
                            .setScale(2, BigDecimal.ROUND_HALF_UP));
        }
    };

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
            .registerModule(new SimpleModule()
                    // beautify double values
                    .addSerializer(Double.class, DOUBLE_SERIALIZER));

    @Override
    public T fromBytes(final byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, getSerializedType());
        } catch (Exception e) {
            throw new IllegalStateException(format("Could not deserialize %s from bytes", getSerializedType()), e);
        }
    }

    @Override
    public byte[] toBytes(final T t) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(t);
        } catch (Exception e) {
            throw new IllegalStateException(format("Could not serialize %s to bytes", getSerializedType()), e);
        }
    }

    protected abstract Class<T> getSerializedType();

}
