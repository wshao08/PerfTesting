
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class WorkThread implements Runnable {
  private String fileName;
  private static int fetchsize = 5000;
  private static String pathPrefix = "root.perf.";
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

  public WorkThread(String file) {
    this.fileName = file;
    this.session = new Session("172.31.28.118", 6667, "root", "root");
    try {
      session.open(false);
    } catch (IoTDBConnectionException e) {
      System.out.println(e.getMessage());
    }
  }

  @Override
  public void run() {
    File inFile = new File(fileName);
    String line = "";
    List<String> deviceIds = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    long count = 0;
    long totalInsertTime = 0;
    long start = System.nanoTime();
    try {
      BufferedReader reader = new BufferedReader(new FileReader(inFile));
      while((line = reader.readLine())!= null){
        String item[] = line.split(",");
        String device = item[0];
        deviceIds.add(pathPrefix + device);
        String time = item[1];
        times.add(Long.parseLong(time.trim()));
        if (times.size() == fetchsize) {
          insertRecords(session, deviceIds, times);
          deviceIds.clear();
          times.clear();
          count += fetchsize;
        }
      }
      totalInsertTime += insertRecords(session, deviceIds, times);
      count += deviceIds.size();
      reader.close();
      this.session.close();
    } catch (IOException | StatementExecutionException | IoTDBConnectionException ex) {
      System.out.println("error");
    }
    long end = System.nanoTime();
    long totaltime = end - start;
    System.out.println("Thread " + Thread.currentThread().getName() + " spent " + totaltime / 1000000 + " miliseconds to insert " + count + " rows. " + "Pure insertion time: " + totalInsertTime / 1000000);
  }

  private long insertRecords(Session session, List<String> deviceIds, List<Long> times) throws StatementExecutionException, IoTDBConnectionException {
    int num = deviceIds.size();
    List<List<String>> measurements = new ArrayList<>();
    List<List<TSDataType>> typeLists = new ArrayList<>();
    List<List<Object>> valueLists = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      measurements.add(Arrays.asList(vehicles));
      typeLists.add(Arrays.asList(dataTypes));
      List<Object> values = new ArrayList<>();
      for (TSDataType t : dataTypes) {
        if (t == TSDataType.INT32) {
          values.add(rand.nextInt(10000));
        } else if (t == TSDataType.DOUBLE){
          values.add(rand.nextDouble());
        } else {
          throw new RuntimeException("Unsupported data types:" + t.toString());
        }
      }
      valueLists.add(values);
    }
    long start = System.nanoTime();
    session.insertRecords(deviceIds, times, measurements, typeLists, valueLists);
    long end = System.nanoTime();
    return end - start;
  }
}
