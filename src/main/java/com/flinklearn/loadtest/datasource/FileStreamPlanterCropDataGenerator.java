package com.flinklearn.loadtest.datasource;

import com.flinklearn.loadtest.model.Device;
import com.flinklearn.loadtest.model.DeviceData;
import org.apache.commons.io.FileUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/****************************************************************************
 * This Generator generates a series of data files in the raw_data folder
 * It is an audit trail data source.
 * This can be used for streaming consumption of data by Flink
 ****************************************************************************/

public class FileStreamPlanterCropDataGenerator implements Runnable {


    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public String planterId;

    public FileStreamPlanterCropDataGenerator(String planterId) {
        this.planterId = planterId;
    }

    public void run() {

        try {

            //Define the data directory to output the files
            String dataDir = "data/raw_planter_crop_data";

            //Clean out existing files in the directory
            FileUtils.cleanDirectory(new File(dataDir));

            //Define a random number generator
            Random random = new Random();

            //Generate 100 sample audit records, one per each file
            for(int i=0; i < 100; i++) {

                //Capture current timestamp
                String currentTime = String.valueOf(System.currentTimeMillis());
                //Generate a random speed value
                String crop = String.valueOf(random.nextInt(2));

                ObjectMapper objectMapper = new ObjectMapper();
                DeviceData data = new DeviceData();
                data.setKey("crop_category");
                data.setValue(crop);
                List<DeviceData> arr = new ArrayList<>();
                arr.add(data);
                Device device = new Device(planterId,"Planter Crop", arr, currentTime);
                objectMapper.writeValue(new File(dataDir + "/planter_trail_" + i + ".json"), device);


//                System.out.println(ANSI_BLUE + "FileStream Generator : Creating File : "
//                            + objectMapper.writeValueAsString(device) + ANSI_RESET);

                //Sleep for a random time ( 1 - 3 secs) before the next record.
                Thread.sleep(random.nextInt(60) + 170);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
