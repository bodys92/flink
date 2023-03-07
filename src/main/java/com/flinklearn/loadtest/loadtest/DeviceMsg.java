package com.flinklearn.loadtest.loadtest;
import org.apache.flink.api.java.tuple.Tuple2;
import java.util.List;

public class DeviceMsg {
    String code;
    List<Tuple2<String,Double>> data;
    String desc;
    long timestamp;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public List<Tuple2<String, Double>> getData() {
        return data;
    }

    public void setData(List<Tuple2<String, Double>> data) {
        this.data = data;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public DeviceMsg() {
    }

    @Override
    public String toString() {
        return "DeviceMsg{" +
                "code='" + code + '\'' +
                ", data=" + data +
                ", desc='" + desc + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
