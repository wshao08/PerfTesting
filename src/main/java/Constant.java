import java.util.Random;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class Constant {

  public static int fetchsize = 200;
  public static String pathPrefix = "root.perf.";
  public static int threadNum = 12;


  public static final int tabletRowNumber = 60;
  public static final int tabletsBatchNumber = 100;


  public static String[] vehicles = {
      "accelerationFB", "accelerationLR", "speed_TypeA", "steeringAngle_TypeA", "EngineRPM_TypeA",
      "TirePressureFL_kpa", "FuelGageIndication", "latitude", "longitude", "AccelPedalAngle_TypeA",
      "TirePressureFR_kpa", "TirePressureRL_kpa", "TirePressureRR_kpa", "AmbientTemperature",
      "TemperatureD",
      "turnLampSwitchStatus", "ATShiftPosition", "BrakePedal", "DoorOpenD", "ParkingBrake",
      "EcoModeIndicator",
      "PowerModeSelect_TypeA", "SportModeSelect", "WindowPositionD", "AirConIndicator",
      "Odometer_km", "HeadLamp_TypeB"
  };
  public static TSDataType[] dataTypes = {
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
}
