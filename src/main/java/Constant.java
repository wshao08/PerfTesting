
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class Constant {


  public static final int STORAGE_GROUP_NUM = 3;

  private Constant() {
  }

  public static final int TABLET_ROW_NUMBER = 60;
  public static final int THREAD_NUM = 12;


  public static final String[] MEASUREMENT_NAMES = {
      "accelerationFB", "accelerationLR", "speed_TypeA", "steeringAngle_TypeA", "EngineRPM_TypeA",
      "TirePressureFL_kpa", "FuelGageIndication", "latitude", "longitude", "AccelPedalAngle_TypeA",
      "TirePressureFR_kpa", "TirePressureRL_kpa", "TirePressureRR_kpa", "AmbientTemperature",
      "TemperatureD",
      "turnLampSwitchStatus", "ATShiftPosition", "BrakePedal", "DoorOpenD", "ParkingBrake",
      "EcoModeIndicator",
      "PowerModeSelect_TypeA", "SportModeSelect", "WindowPositionD", "AirConIndicator",
      "Odometer_km", "HeadLamp_TypeB"
  };
  public final static TSDataType[] DATA_TYPES = {
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

  public final static TSEncoding[] ENCODINGS = {
      TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN,
      TSEncoding.PLAIN,
      TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN,
      TSEncoding.PLAIN,
      TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN,
      TSEncoding.PLAIN,
      TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN,
      TSEncoding.PLAIN,
      TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN
  };


  //  public static int fetchsize = 200;
  public static final String PATH_PREFIX = "root.perf_";
  public static final String DOT = ".";

  public static final int LOG_FREQ = 1000;

  public static String HOST = "172.31.28.118";
//  public static final String HOST = "127.0.0.1";


  public static int MULTI_THREAD_NUMBER = 72;
}
