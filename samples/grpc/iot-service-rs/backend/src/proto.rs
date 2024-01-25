// Protobuf concerns.

use prost::Name;

include!(concat!(env!("OUT_DIR"), "/iot.registration.rs"));

// Until a time where Prost generates this meta info, we must provide it manually.
impl Name for Registered {
    const NAME: &'static str = "Registered";
    const PACKAGE: &'static str = "iot.registration";

    fn full_name() -> String {
        format!("{}.{}", Self::PACKAGE, Self::NAME)
    }

    fn type_url() -> String {
        format!("type.googleapis.com/{}", Self::full_name())
    }
}

include!(concat!(env!("OUT_DIR"), "/iot.temperature.rs"));

// Until a time where Prost generates this meta info, we must provide it manually.
impl Name for TemperatureRead {
    const NAME: &'static str = "TemperatureRead";
    const PACKAGE: &'static str = "iot.temperature";

    fn full_name() -> String {
        format!("{}.{}", Self::PACKAGE, Self::NAME)
    }

    fn type_url() -> String {
        format!("type.googleapis.com/{}", Self::full_name())
    }
}
