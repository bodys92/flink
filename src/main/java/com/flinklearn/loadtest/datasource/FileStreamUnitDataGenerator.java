package com.flinklearn.loadtest.datasource;

import com.flinklearn.loadtest.model.Device;
import com.flinklearn.loadtest.model.DeviceData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FileStreamUnitDataGenerator implements Runnable {
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public String id;



    public FileStreamUnitDataGenerator(String id) {this.id = id;}

    @Override
    public void run() {
        try {

            List<Tuple2<String, String>> unitMeasurement = new ArrayList<>();
            unitMeasurement.add(new Tuple2<>("planting_depth", "Detailed description"));
            unitMeasurement.add(new Tuple2<>("soil_moisture", "Detailed description"));
            unitMeasurement.add(new Tuple2<>("soil_temperature", "Detailed description"));
            unitMeasurement.add(new Tuple2<>("seed_count", "Detailed description"));
            unitMeasurement.add(new Tuple2<>("air_pressure", "Detailed description"));
            unitMeasurement.add(new Tuple2<>("seed_pressure", "Detailed description"));
            unitMeasurement.add(new Tuple2<>("fertilizer_pressure", "Detailed description"));

            for (Tuple2<String,String> mes: unitMeasurement
                 ) {
                File dataDir = new File(String.format("data/raw_unit_%s", mes.f0));

                if (!dataDir.exists()){
                    dataDir.mkdirs();
                } else {
                }
            }

            Random random = new Random();

            for (int i=0; i < 100; i++){

                for (Tuple2<String,String> mes: unitMeasurement
                ) {
                    File dataDir = new File(String.format("data/raw_unit_%s", mes.f0));
                    String eventTime = String.valueOf(System.currentTimeMillis());
                    String value = String.valueOf(random.nextInt(100) + 1);

                    ObjectMapper mapper = new ObjectMapper();
                    DeviceData data = new DeviceData();
                    data.setKey(mes.f0);
                    data.setValue(value);
                    List<DeviceData> dataList = new ArrayList<>();
                    dataList.add(data);
                    Device unit = new Device(
                            String.format("unit_%s", id),
                            mes.f1,
                            dataList,
                            eventTime);

                    mapper.writeValue(new File(dataDir.getPath() + "/unit" + id + "_trail" + i + ".json"), unit);
                    System.out.println(ANSI_GREEN + "FileStream Generator : Creating File : "
                            + mapper.writeValueAsString(unit) + ANSI_RESET);
                }

                Thread.sleep(random.nextInt(60) + 170);
            }


        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
