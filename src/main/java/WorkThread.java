
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class WorkThread implements Runnable {

  private String fileName;
  private static int fetchsize = 200;
  private static String pathPrefix = "root.perf.";
  private final Random rand = new Random();
  private Session session;
  private static String vehicles[] = {
      "accelerationFB", "accelerationLR", "speed_TypeA", "steeringAngle_TypeA", "EngineRPM_TypeA",
      "TirePressureFL_kpa", "FuelGageIndication", "latitude", "longitude", "AccelPedalAngle_TypeA",
      "TirePressureFR_kpa", "TirePressureRL_kpa", "TirePressureRR_kpa", "AmbientTemperature",
      "TemperatureD",
      "turnLampSwitchStatus", "ATShiftPosition", "BrakePedal", "DoorOpenD", "ParkingBrake",
      "EcoModeIndicator",
      "PowerModeSelect_TypeA", "SportModeSelect", "WindowPositionD", "AirConIndicator",
      "Odometer_km", "HeadLamp_TypeB"
  };
  private static TSDataType dataTypes[] = {
      TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE,
      TSDataType.DOUBLE,
      TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE,
      TSDataType.DOUBLE,
      TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.INT32, TSDataType.INT32,
      TSDataType.INT32,
      TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32,
      TSDataType.INT32,
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
    List<Long> minuteIds = new ArrayList<>();
    long count = 0;
    long totalInsertTime = 0;
    long start = System.nanoTime();
    try {
      BufferedReader reader = new BufferedReader(new FileReader(inFile));
      while ((line = reader.readLine()) != null) {
        String item[] = line.split(",");
        String device = item[0];
        deviceIds.add(pathPrefix + device);
        String minuteId = item[1];
        minuteIds.add(Long.parseLong(minuteId.trim()));
        if (minuteIds.size() == fetchsize) {
          totalInsertTime += insertRecords(session, deviceIds, minuteIds);
          deviceIds.clear();
          minuteIds.clear();
          count += (Constant.tabletRowNumber * fetchsize);
        }
      }
      totalInsertTime += insertRecords(session, deviceIds, minuteIds);
      count += (deviceIds.size() * Constant.tabletRowNumber);
      reader.close();
      this.session.close();
    } catch (IOException | StatementExecutionException | IoTDBConnectionException ex) {
      System.out.println("error" + ex.getMessage());
    }
    long end = System.nanoTime();
    long totaltime = end - start;
    System.out.println(
        "Thread " + Thread.currentThread().getName() + " spent " + totaltime / 1000000
            + " miliseconds to insert " + count + " rows. " + "Pure insertion time: "
            + totalInsertTime / 1000000);
  }

  private long insertRecords(Session session, List<String> deviceIds, List<Long> times)
      throws StatementExecutionException, IoTDBConnectionException {

    long start;
    long duration = 0;
    List<MeasurementSchema> schemas = new ArrayList<>();
    for (int i = 0; i < vehicles.length; i++) {
      schemas.add(new MeasurementSchema(vehicles[i], dataTypes[i]));
    }

    Map<String, Tablet> tablets = new HashMap<>();

    for (int i = 0; i < deviceIds.size(); i++) {
      String deviceId = deviceIds.get(i);
      long minuteId = times.get(i);
      Tablet tablet = new Tablet(deviceId, schemas);

      // create a tablet
      for (int row = 0; row < Constant.tabletRowNumber; row++) {
        int rowId = tablet.rowSize++;
        tablet.addTimestamp(rowId, minuteId * 60 + row);
        for (int col = 0; col < dataTypes.length; col++) {
          tablet.addValue(vehicles[col], rowId, getNext(col, dataTypes));
        }
      }
      tablets.put(deviceId, tablet);
      if (tablets.size() == Constant.tabletsBatchNumber) {
        start = System.nanoTime();
        session.insertTablets(tablets, true);
        duration += (System.nanoTime() - start);
        tablets.clear();
      }
    }
    if (!tablets.isEmpty()) {
      start = System.nanoTime();
      session.insertTablets(tablets, true);
      duration += (System.nanoTime() - start);
    }
    return duration;
  }

  public Object getNext(int index, TSDataType[] types) {
    if (types[index] == TSDataType.INT32) {
      return rand.nextInt(10000);
    } else if (types[index] == TSDataType.DOUBLE) {
      return rand.nextDouble();
    } else {
      throw new RuntimeException("Unsupported data types:" + types[index].toString());
    }
  }
}
