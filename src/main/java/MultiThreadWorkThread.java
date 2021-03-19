import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

public class MultiThreadWorkThread implements Runnable {


  private final String fileName;
  private final Random rand = new Random();
  private final Map<Integer, Session> sessionMap;
  private final VectorMeasurementSchema measurementSchemas = new VectorMeasurementSchema(
      Constant.MEASUREMENT_NAMES, Constant.DATA_TYPES);
  Tablet tablet = new Tablet(null, Collections.singletonList(measurementSchemas), 60);

  public MultiThreadWorkThread(String fileName) {
    this.fileName = fileName;
    this.sessionMap = new HashMap<>();
    try {
      Session[] sessions = new Session[3];
      Session session1 = new Session(Constant.HOST, 6667, "root", "root");
      session1.open(false);
      Session session2 = new Session(Constant.HOST1, 6667, "root", "root");
      session2.open(false);
      Session session3 = new Session(Constant.HOST2, 6667, "root", "root");
      session3.open(false);
      sessions[0] = session1;
      sessions[1] = session2;
      sessions[2] = session3;
      for (int i = 0; i < Constant.STORAGE_GROUP_NUM; i++) {
        sessionMap.put(i, sessions[i % 3]);
      }
    } catch (IoTDBConnectionException e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    File inFile = new File(fileName);
    String line = "";
    long count = 0;
    long duration = 0;
    String deviceId;
    int row_counter = 0;
    try {
      BufferedReader reader = new BufferedReader(new FileReader(inFile));
      while ((line = reader.readLine()) != null) {
        row_counter++;
        String[] item = line.split(",");
        String device = item[0];
        int storage_group = Integer.parseInt(device) % Constant.STORAGE_GROUP_NUM;
        deviceId = Constant.PATH_PREFIX + storage_group + Constant.DOT + device;
        long minuteId = Long.parseLong(item[1].trim());
        duration += generateAndInsertTablet(deviceId, minuteId,storage_group);
        count += Constant.TABLET_ROW_NUMBER;
        if (row_counter % Constant.LOG_FREQ == 0) {
          printResult(count, duration, false);
        }
      }
      reader.close();
      for (Session session : sessionMap.values()) {
        session.close();
      }
    } catch (IOException | IoTDBConnectionException ex) {
      System.out.println("error: " + ex.getMessage());
    }
    printResult(count, duration, true);
  }

  public long generateAndInsertTablet(String deviceId, long minuteId,int storage_group) {
    // create a tablet
//    Tablet tablet = new Tablet(deviceId, Collections.singletonList(measurementSchemas), 60);
    tablet.setDeviceId(deviceId);
    tablet.reset();
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    for (int innerMinute = 0; innerMinute < Constant.TABLET_ROW_NUMBER; innerMinute++) {
      int rowId = tablet.rowSize++;
      timestamps[rowId] = minuteId * 60 + innerMinute;
      for (int col = 0; col < Constant.DATA_TYPES.length; col++) {
        switch (Constant.DATA_TYPES[col]) {
          case INT32:
            int[] sensor = (int[]) values[col];
            sensor[rowId] = (int) getNext(col, Constant.DATA_TYPES);
            break;
          case DOUBLE:
            double[] sensors = (double[]) values[col];
            sensors[rowId] = (double) getNext(col, Constant.DATA_TYPES);
            break;
          default:
            throw new IllegalArgumentException("DataType not allowed");
        }
      }
    }
    // insert a tablet
    long start = System.nanoTime();
    try {
      sessionMap.get(storage_group).insertTablet(tablet, true);
    } catch (Throwable t) {
      t.printStackTrace();
    }
    return System.nanoTime() - start;
  }

  public void printResult(long count, long duration, boolean isFinal) {
    if (isFinal) {
      System.out.print(">>>>> Final ");
    }
    System.out.println(
        "Thread " + Thread.currentThread().getName() + ", " + count + " rows. "
            + "Pure insertion time: " + duration / 1000000 + ", speed: " + (count / (duration
            / 1000000000.0)) + "r/s");
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
