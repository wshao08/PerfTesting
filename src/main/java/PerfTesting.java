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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PerfTesting {


  public static void main(String[] args) throws InterruptedException {
    String dirpath;
    if (args.length == 0) {
      dirpath = "./";
    } else {
      dirpath = args[0];
    }
    ExecutorService executor = Executors.newFixedThreadPool(Constant.THREAD_NUM);
    for (int i = 0; i < Constant.THREAD_NUM; i++) {
      String filename = dirpath + File.separator + "init_data_" + i + ".txt";
      executor.submit(new WorkThread(filename));
    }
    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    System.out.println("Testing finishes.");
  }

}