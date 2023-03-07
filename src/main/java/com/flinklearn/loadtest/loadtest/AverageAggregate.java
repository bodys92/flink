package com.flinklearn.loadtest.loadtest;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;


import java.util.ArrayList;
import java.util.List;

// implementation of an aggregation function for an 'average'
public class AverageAggregate implements AggregateFunction<DeviceMsg, Tuple4<Tuple3<String, String,String>, Double, Long, Integer>, DeviceMsg> {

    public Tuple4<Tuple3<String, String,String>, Double, Long, Integer> createAccumulator() {
        return new Tuple4<>(new Tuple3<>("","",""), 0d, 0L, 0);
    }

    @Override
    public Tuple4<Tuple3<String, String,String>, Double, Long, Integer> add(DeviceMsg msg, Tuple4<Tuple3<String, String,String>, Double, Long, Integer> acc) {
        Tuple4<Tuple3<String, String, String>, Double, Long, Integer> tuple3DoubleLongIntegerTuple4 = new Tuple4<>(
                new Tuple3<>(
                        msg.getCode(),
                        msg.getDesc(),
                        msg.getData().get(0).f0
                ),
                acc.f1 + msg.getData().get(0).f1,
                msg.getTimestamp(),
                acc.f3 + 1);
        return tuple3DoubleLongIntegerTuple4;
    }

    @Override
    public Tuple4<Tuple3<String, String,String>, Double, Long, Integer> merge(Tuple4<Tuple3<String, String,String>, Double, Long, Integer> a, Tuple4<Tuple3<String, String,String>, Double, Long, Integer> b) {

        return new Tuple4<>(new Tuple3<>(a.f0.f0,a.f0.f1,a.f0.f2), a.f1 + b.f1, Math.min(a.f2, b.f2), a.f3 + b.f3);
    }

    @Override
    public DeviceMsg getResult(Tuple4<Tuple3<String, String,String>, Double, Long, Integer> acc) {
        DeviceMsg msg = new DeviceMsg();
        msg.setCode(acc.f0.f0);
        msg.setDesc(acc.f0.f1);
        List<Tuple2<String, Double>> tmpList = new ArrayList<>();
        tmpList.add(new Tuple2<String, Double>(acc.f0.f2,acc.f1 / acc.f3));
        msg.setData(tmpList);
        msg.setTimestamp(acc.f2);
        System.out.println("--- Agregate: " + msg.toString());
        return msg;
    }
}

