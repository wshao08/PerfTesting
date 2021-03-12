/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.h2.store.fs.FilePath;

public class PerfTesting {
  private static final int TIME_SERIES_NUM = 3;
  private static int fetchsize = 5;
  private static String pathPrefix = "root.perf.";
  private static int threadNum = 12;
  private final Random rand = new Random();
  private Session session;

  private static String vehicles[] = {
          "accelerationFB", "accelerationLR", "speed_TypeA", "steeringAngle_TypeA", "EngineRPM_TypeA",
          "TirePressureFL_kpa", "FuelGageIndication", "latitude", "longitude", "AccelPedalAngle_TypeA",
          "TirePressureFR_kpa", "TirePressureRL_kpa", "TirePressureRR_kpa", "AmbientTemperature", "TemperatureD",
          "turnLampSwitchStatus", "ATShiftPosition", "BrakePedal", "DoorOpenD", "ParkingBrake", "EcoModeIndicator",
          "PowerModeSelect_TypeA", "SportModeSelect", "WindowPositionD", "AirConIndicator", "Odometer_km", "HeadLamp_TypeB"
  };
  private static TSDataType dataTypes[] = {
          TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE,
          TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE,
          TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32,
          TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32,
          TSDataType.INT32, TSDataType.INT32, TSDataType.INT32
  };

  public PerfTesting(Session session) {
    this.session = session;
  }


  public static void main(String[] args) throws InterruptedException {
    String dirpath;
    if (args.length == 0) {
      dirpath = "./";
    } else {
      dirpath = args[0];
    }
    ExecutorService executor = Executors.newFixedThreadPool(threadNum);
    for (int i = 0; i < threadNum; i++) {
      String filename = dirpath + File.separator + "init_data_" + i + ".txt";
      executor.submit(new WorkThread(filename));
    }
    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    System.out.println("Testing finishes.");
  }

}