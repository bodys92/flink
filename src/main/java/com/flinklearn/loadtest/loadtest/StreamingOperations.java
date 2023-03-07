package com.flinklearn.loadtest.loadtest;
import com.flinklearn.loadtest.common.Utils;
import com.flinklearn.loadtest.datasource.FileStreamPlanterCropDataGenerator;
import com.flinklearn.loadtest.datasource.FileStreamPlanterSpeedDataGenerator;
import com.flinklearn.loadtest.datasource.FileStreamUnitDataGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

/*
A Flink Program that reads a AWS Kinesis stream, computes a Aggregate and Join operation,
and writes to a S3 output
 */

public class StreamingOperations {

    private static DataStream<String> getUnitStream(String dataDir, StreamExecutionEnvironment env){

        dataDir = "data/" + dataDir;

        TextInputFormat dataFormat = new TextInputFormat(
                new org.apache.flink.core.fs.Path(dataDir));

        return env.readFile(dataFormat,
                dataDir,    //Director to monitor
                FileProcessingMode.PROCESS_CONTINUOUSLY,
                200); //monitor interval
    }

    private static KeyedStream<DeviceMsg, String> getUnitObjStream(DataStream<String> stream) {
        return stream
                .map(new MapFunction<String,DeviceMsg>() {
                    @Override
                    public DeviceMsg map(String stream) throws IOException {
                        //System.out.println("--- Received Record : " + stream);
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode node = mapper.readTree(stream);
                        DeviceMsg device = new DeviceMsg();
                        device.setCode(node.get("id").textValue());
                        device.setDesc(node.get("desc").textValue());
                        device.setTimestamp(Long.valueOf(node.get("timestamp").textValue()));
                        List<Tuple2<String,Double>> data = new ArrayList<>();
                        data.add(new Tuple2<>(node.get("data").findValuesAsText("key").get(0),
                                Double.valueOf(node.get("data").findValuesAsText("value").get(0))));
                        device.setData(data);
                        //System.out.println("--- Send Record : " + mapper.writeValueAsString(device));
                        return device;
                    }
                }).assignTimestampsAndWatermarks(
                        (new AssignerWithPunctuatedWatermarks<DeviceMsg>() {

                            //Extract Event timestamp value.
                            @Override
                            public long extractTimestamp(
                                    DeviceMsg deviceMsg,
                                    long previousTimeStamp) {

                                return deviceMsg.getTimestamp();
                            }

                            //Extract Watermark
                            transient long currWaterMark = 0L;
                            int delay = 2000;
                            int buffer = 500;

                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(
                                    DeviceMsg deviceMsg,
                                    long newTimestamp) {

                                long currentTime = System.currentTimeMillis();
                                if (currWaterMark == 0L) {
                                    currWaterMark = currentTime;
                                }
                                //update watermark every 2 seconds
                                else if ( currentTime - currWaterMark > delay) {
                                    currWaterMark = currentTime;
                                }
                                //return watermark adjusted to buffer
                                return new Watermark(
                                        currWaterMark - buffer);

                            }
                        })
                ).keyBy((new KeySelector<DeviceMsg, String>() {
                    @Override
                    public String getKey(DeviceMsg msg) {
                        return msg.getCode();
                    }
                }))
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2000)))
                .aggregate(new AverageAggregate())
                .keyBy((new KeySelector<DeviceMsg, String>() {
                    @Override
                    public String getKey(DeviceMsg msg) {
                        return msg.getCode();
                    }
                }));
    }

    public static void main(String[] args) {

        try{

            /****************************************************************************
             *                 Setup Flink environment.
             ****************************************************************************/
            Configuration cfg = new Configuration();
            int defaultParallelism = Runtime.getRuntime().availableProcessors();
            cfg.setString("taskmanager.network.numberOfBuffers", "5000");
            final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment(defaultParallelism, cfg);

            /****************************************************************************
             *                  Read Kinesis Stream into a DataStream.
             ****************************************************************************/

            DataStream<String> unitAirPressStrStream = getUnitStream("raw_unit_air_pressure", streamEnv);
            DataStream<String> unitFerPressStrStream = getUnitStream("raw_unit_fertilizer_pressure", streamEnv);
            DataStream<String> unitPlanDepthStrStream = getUnitStream("raw_unit_planting_depth", streamEnv);
            DataStream<String> unitSeedPressStrStream = getUnitStream("raw_unit_seed_count", streamEnv);
            DataStream<String> unitSeedCountStrStream = getUnitStream("raw_unit_seed_pressure", streamEnv);
            DataStream<String> unitSoilMosStrStream = getUnitStream("raw_unit_soil_moisture", streamEnv);
            DataStream<String> unitSoilTempStrStream = getUnitStream("raw_unit_soil_temperature", streamEnv);

            KeyedStream<DeviceMsg, String> unitAirPressObjStream = getUnitObjStream(unitAirPressStrStream);
            KeyedStream<DeviceMsg, String> unitFerPressObjStream = getUnitObjStream(unitFerPressStrStream);
            KeyedStream<DeviceMsg, String> unitPlanDepthObjStream = getUnitObjStream(unitPlanDepthStrStream);
            KeyedStream<DeviceMsg, String> unitSeedPressObjStream = getUnitObjStream(unitSeedPressStrStream);
            KeyedStream<DeviceMsg, String> unitSeedCountObjStream = getUnitObjStream(unitSeedCountStrStream);
            KeyedStream<DeviceMsg, String> unitSoilMosObjStream = getUnitObjStream(unitSoilMosStrStream);
            KeyedStream<DeviceMsg, String> unitSoilTempObjStream = getUnitObjStream(unitSoilTempStrStream);

            DataStream<DeviceMsg> joinedStream = unitAirPressObjStream
                    .join(unitFerPressObjStream)
                    .where(unitAirPressObjStream.getKeySelector())
                    .equalTo(unitFerPressObjStream.getKeySelector())
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(2000)))
                    .apply(new JoinFunction<DeviceMsg, DeviceMsg, DeviceMsg>() {
                        @Override
                        public DeviceMsg join(DeviceMsg deviceMsg, DeviceMsg deviceMsg2) throws Exception {
                            DeviceMsg msg = new DeviceMsg();
                            msg.setCode(deviceMsg.getCode());
                            msg.setDesc(deviceMsg.getDesc());
                            List<Tuple2<String,Double>> data = new ArrayList<>();
                            data.addAll(deviceMsg.getData());
                            data.addAll(deviceMsg2.getData());
                            msg.setData(data);
                            msg.setTimestamp(Math.max(deviceMsg.getTimestamp(), deviceMsg2.getTimestamp()));
                            return msg;
                        }
                    }).join(unitPlanDepthObjStream)
                    .where(unitAirPressObjStream.getKeySelector())
                    .equalTo(unitPlanDepthObjStream.getKeySelector())
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(2000)))
                    .apply(new JoinFunction<DeviceMsg, DeviceMsg, DeviceMsg>() {
                        @Override
                        public DeviceMsg join(DeviceMsg deviceMsg, DeviceMsg deviceMsg2) throws Exception {
                            DeviceMsg msg = new DeviceMsg();
                            msg.setCode(deviceMsg.getCode());
                            msg.setDesc(deviceMsg.getDesc());
                            List<Tuple2<String,Double>> data = new ArrayList<>();
                            data.addAll(deviceMsg.getData());
                            data.addAll(deviceMsg2.getData());
                            msg.setData(data);
                            msg.setTimestamp(Math.max(deviceMsg.getTimestamp(), deviceMsg2.getTimestamp()));
                            return msg;
                        }
                    }).join(unitSeedPressObjStream)
                    .where(unitAirPressObjStream.getKeySelector())
                    .equalTo(unitSeedPressObjStream.getKeySelector())
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(2000)))
                    .apply(new JoinFunction<DeviceMsg, DeviceMsg, DeviceMsg>() {
                        @Override
                        public DeviceMsg join(DeviceMsg deviceMsg, DeviceMsg deviceMsg2) throws Exception {
                            DeviceMsg msg = new DeviceMsg();
                            msg.setCode(deviceMsg.getCode());
                            msg.setDesc(deviceMsg.getDesc());
                            List<Tuple2<String,Double>> data = new ArrayList<>();
                            data.addAll(deviceMsg.getData());
                            data.addAll(deviceMsg2.getData());
                            msg.setData(data);
                            msg.setTimestamp(Math.max(deviceMsg.getTimestamp(), deviceMsg2.getTimestamp()));
                            return msg;
                        }
                    }).join(unitSeedCountObjStream)
                    .where(unitAirPressObjStream.getKeySelector())
                    .equalTo(unitSeedCountObjStream.getKeySelector())
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(2000)))
                    .apply(new JoinFunction<DeviceMsg, DeviceMsg, DeviceMsg>() {
                        @Override
                        public DeviceMsg join(DeviceMsg deviceMsg, DeviceMsg deviceMsg2) throws Exception {
                            DeviceMsg msg = new DeviceMsg();
                            msg.setCode(deviceMsg.getCode());
                            msg.setDesc(deviceMsg.getDesc());
                            List<Tuple2<String,Double>> data = new ArrayList<>();
                            data.addAll(deviceMsg.getData());
                            data.addAll(deviceMsg2.getData());
                            msg.setData(data);
                            msg.setTimestamp(Math.max(deviceMsg.getTimestamp(), deviceMsg2.getTimestamp()));
                            return msg;
                        }
                    }).join(unitSoilMosObjStream)
                    .where(unitAirPressObjStream.getKeySelector())
                    .equalTo(unitSoilMosObjStream.getKeySelector())
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(2000)))
                    .apply(new JoinFunction<DeviceMsg, DeviceMsg, DeviceMsg>() {
                        @Override
                        public DeviceMsg join(DeviceMsg deviceMsg, DeviceMsg deviceMsg2) throws Exception {
                            DeviceMsg msg = new DeviceMsg();
                            msg.setCode(deviceMsg.getCode());
                            msg.setDesc(deviceMsg.getDesc());
                            List<Tuple2<String,Double>> data = new ArrayList<>();
                            data.addAll(deviceMsg.getData());
                            data.addAll(deviceMsg2.getData());
                            msg.setData(data);
                            msg.setTimestamp(Math.max(deviceMsg.getTimestamp(), deviceMsg2.getTimestamp()));
                            return msg;
                        }
                    }).join(unitSoilTempObjStream)
                    .where(unitAirPressObjStream.getKeySelector())
                    .equalTo(unitSoilTempObjStream.getKeySelector())
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(2000)))
                    .apply(new JoinFunction<DeviceMsg, DeviceMsg, DeviceMsg>() {
                        @Override
                        public DeviceMsg join(DeviceMsg deviceMsg, DeviceMsg deviceMsg2) throws Exception {
                            DeviceMsg msg = new DeviceMsg();
                            msg.setCode(deviceMsg.getCode());
                            msg.setDesc(deviceMsg.getDesc());
                            List<Tuple2<String,Double>> data = new ArrayList<>();
                            data.addAll(deviceMsg.getData());
                            data.addAll(deviceMsg2.getData());
                            msg.setData(data);
                            msg.setTimestamp(Math.max(deviceMsg.getTimestamp(), deviceMsg2.getTimestamp()));
                            System.out.println("--- Joined Record : " + msg.toString());
                            return msg;
                        }
                    });

            /****************************************************************************
             *                  Setup data source and execute the Flink pipeline
             ****************************************************************************/

            //Define the output directory to store summary information
            String outputDir = "data/five_sec_summary";
            //Clean out existing files in the directory
            FileUtils.cleanDirectory(new File(outputDir));

            //Setup a streaming file sink to the output directory
            final StreamingFileSink<DeviceMsg> outSink
                    = StreamingFileSink
                    .forRowFormat(new Path(outputDir),
                            new SimpleStringEncoder<DeviceMsg>
                                    ("UTF-8"))
                    .build();

            //Add the file sink as sink to the DataStream.
            joinedStream.addSink(outSink);

            //Start the File Stream generator on a separate thread
            Utils.printHeader("Starting File Device Data Generator...");
            List<Thread> threads = new ArrayList<>();
            threads.add(new Thread(new FileStreamPlanterSpeedDataGenerator("planter_001")));
            threads.add(new Thread(new FileStreamPlanterCropDataGenerator("planter_001")));

            for(int i = 0; i<20; i++){
                threads.add(new Thread(new FileStreamUnitDataGenerator(String.valueOf(i))));
            }

            for (Thread genThread: threads
                 ) {
                genThread.start();
            }

            // execute the streaming pipeline
            streamEnv.execute("EDU Flink Streaming Kinesis to S3");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
