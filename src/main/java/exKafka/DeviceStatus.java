package exKafka;

import java.io.Serializable;

public class DeviceStatus implements Serializable {

    private long timestamp;
    private String device;
    private Double value;

    public DeviceStatus() {
    }

    // getters and setters needed for jackson

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    public DeviceStatus withTimestamp(final long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(final String device) {
        this.device = device;
    }

    public DeviceStatus withDevice(final String device) {
        this.device = device;
        return this;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(final Double value) {
        this.value = value;
    }

    public DeviceStatus withValue(final Double value) {
        this.value = value;
        return this;
    }
}
