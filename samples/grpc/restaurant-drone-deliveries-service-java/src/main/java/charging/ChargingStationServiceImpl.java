package charging;

import charging.proto.*;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChargingStationServiceImpl implements ChargingStationService {

  private static final Logger logger = LoggerFactory.getLogger(ChargingStationServiceImpl.class);

  @Override
  public CompletionStage<CreateChargingStationResponse> createChargingStation(
      CreateChargingStationRequest in) {
    return null;
  }

  @Override
  public CompletionStage<GetChargingStationStateResponse> getChargingStationState(
      GetChargingStationStateRequest in) {
    return null;
  }
}
