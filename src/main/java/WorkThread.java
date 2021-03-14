
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.ZoneId;
import java.util.*;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Config;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class WorkThread implements Runnable {

  private String fileName;
  private Session session;
  public final Random rand = new Random();

  public WorkThread(String file) {
    this.fileName = file;
    this.session = new Session("172.31.28.118", 6667, "root", "root", Constant.fetchsize,
        null, 1024 * 1024 * 4, Config.DEFAULT_MAX_FRAME_SIZE, false);
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
        deviceIds.add(Constant.pathPrefix + device);
        String minuteId = item[1];
        minuteIds.add(Long.parseLong(minuteId.trim()));
        if (minuteIds.size() == Constant.fetchsize) {
          totalInsertTime += insertRecords(session, deviceIds, minuteIds);
          deviceIds.clear();
          minuteIds.clear();
          count += (Constant.tabletRowNumber * Constant.fetchsize);
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
        "Thread\t" + Thread.currentThread().getName() + "\tspent " + totaltime / 1000000
            + " ms to insert " + count + " rows. " + "\tPure insertion time: "
            + totalInsertTime / 1000000 + "\t speed: \t" + (count * 28.0) / (totalInsertTime
            / 1000000000.0) + "\t pt/s");
  }

  private long insertRecords(Session session, List<String> deviceIds, List<Long> times)
      throws StatementExecutionException, IoTDBConnectionException {

    long start;
    long duration = 0;
    List<MeasurementSchema> schemas = new ArrayList<>();
    for (int i = 0; i < Constant.vehicles.length; i++) {
      schemas.add(new MeasurementSchema(Constant.vehicles[i], Constant.dataTypes[i]));
    }

    Map<String, Tablet> tablets = new HashMap<>();

    for (int i = 0; i < deviceIds.size(); i++) {
      String deviceId = deviceIds.get(i);
      long minuteId = times.get(i);
      Tablet tablet = new Tablet(deviceId, schemas, Constant.tabletRowNumber);

      // create a tablet
      for (int row = 0; row < Constant.tabletRowNumber; row++) {
        int rowId = tablet.rowSize++;
        tablet.addTimestamp(rowId, minuteId * 60 + row);
        for (int col = 0; col < Constant.dataTypes.length; col++) {
          tablet.addValue(Constant.vehicles[col], rowId, getNext(col, Constant.dataTypes));
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
