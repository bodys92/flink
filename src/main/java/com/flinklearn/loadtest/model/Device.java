package com.flinklearn.loadtest.model;

import java.util.List;


public class Device {
    public String id;
    public String desc;
    List<DeviceData> data;
    public String timestamp;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public List<DeviceData> getData() {
        return data;
    }

    public void setData(List<DeviceData> data) {
        this.data = data;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Device(String id, String desc, List<DeviceData> data, String timestamp) {
        this.id = id;
        this.desc = desc;
        this.data = data;
        this.timestamp = timestamp;
    }
}

