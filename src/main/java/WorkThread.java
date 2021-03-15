import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

public class WorkThread implements Runnable {

  private final String fileName;
  private final Random rand = new Random();
  private final Session session;
  private final VectorMeasurementSchema measurementSchemas = new VectorMeasurementSchema(
      Constant.MEASUREMENT_NAMES, Constant.DATA_TYPES);

  public WorkThread(String file) {
    this.fileName = file;
    this.session = new Session(Constant.HOST, 6667, "root", "root");
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
        int storage_group = Integer.parseInt(device) % 3;
        deviceId = Constant.PATH_PREFIX + storage_group + Constant.DOT + device;
        long minuteId = Long.parseLong(item[1].trim());
        duration += generateAndInsertTablet(deviceId, minuteId);
        count += Constant.TABLET_ROW_NUMBER;
        if (row_counter % Constant.LOG_FREQ == 0) {
          printResult(count, duration, false);
        }
      }
      reader.close();
      this.session.close();
    } catch (IOException | IoTDBConnectionException ex) {
      System.out.println("error: " + ex.getMessage());
    }
    printResult(count, duration, true);
  }

  public long generateAndInsertTablet(String deviceId, long minuteId) {
    // create a tablet
    Tablet tablet = new Tablet(deviceId, Collections.singletonList(measurementSchemas), false);
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
      session.insertTablet(tablet, true);
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
