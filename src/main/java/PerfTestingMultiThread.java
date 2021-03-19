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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class PerfTestingMultiThread {


  public static void main(String[] args)
      throws InterruptedException, IoTDBConnectionException {
    String dirpath;
    boolean toRegister = false;
    String ip;
    if (args.length == 0) {
      dirpath = "./";
    } else {
      dirpath = args[0];
      toRegister = Boolean.parseBoolean(args[1].trim());
      if (args.length > 2) {
        ip = args[2].trim();
        Constant.HOST = ip;
      }
      if (args.length > 3) {
        Constant.MULTI_THREAD_NUMBER = Integer.parseInt(args[3].trim());
      }
    }

    if (toRegister) {
      Session session = new Session(Constant.HOST, 6667, "root", "root");
      session.open();
      String name = "template1";
      List<List<String>> measurements = new ArrayList<>();
      List<String> verMeasurement = Arrays.asList(Constant.MEASUREMENT_NAMES);
      measurements.add(verMeasurement);

      List<List<TSDataType>> dataTypes = new ArrayList<>();
      List<TSDataType> dataType = Arrays.asList(Constant.DATA_TYPES);
      dataTypes.add(dataType);
      List<List<TSEncoding>> encodings = new ArrayList<>();
      List<TSEncoding> encoding = Arrays.asList(Constant.ENCODINGS);
      encodings.add(encoding);

      List<CompressionType> compressors = Collections.singletonList(CompressionType.SNAPPY);

      try {
        session.createDeviceTemplate(name, measurements, dataTypes, encodings, compressors);
        for (int i = 0; i < Constant.STORAGE_GROUP_NUM; i++) {
//          session.setStorageGroup(Constant.PATH_PREFIX + i);
//          System.out.println(">>> set storage group: " + (Constant.PATH_PREFIX + i));
          session.setDeviceTemplate(name, Constant.PATH_PREFIX + i);
        }
      } catch (StatementExecutionException e) {
        e.printStackTrace();
      }
      session.close();
    }

    ExecutorService executor = Executors.newFixedThreadPool(Constant.MULTI_THREAD_NUMBER);
    for (int i = 0; i < Constant.MULTI_THREAD_NUMBER; i++) {
      String filename = dirpath + File.separator + "init_data_" + i + ".txt";
      executor.submit(new MultiThreadWorkThread(filename));
    }
    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    System.out.println("Testing finishes.");
  }

}