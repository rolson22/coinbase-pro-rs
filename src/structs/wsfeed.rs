use super::DateTime;
use crate::utils::{
    f64_from_string, f64_nan_from_string, f64_opt_from_string, uuid_opt_from_string,
};
use serde::{de, Deserialize, Deserializer, Serialize};
use serde::de::{MapAccess, Visitor};
use serde_json::Value;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct Auth {
    pub name: String,
    pub privateKey: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Subscribe {
    #[serde(rename = "type")]
    pub _type: SubscribeCmd,
    pub product_ids: Vec<String>,
    pub channel: ChannelType,
    #[serde(flatten)]
    pub auth: Option<Auth>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub enum SubscribeCmd {
    Subscribe,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(untagged)]
pub enum Channel {
    Name(ChannelType),
    WithProduct {
        name: ChannelType,
        product_ids: Vec<String>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum ChannelType {
    Heartbeat,
    Status,
    Ticker,
    Level2,
    Matches,
    Full,
    User,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub(crate) enum InputMessage {
    Subscriptions {
        channels: Vec<Channel>,
    },
    Heartbeat {
        sequence: usize,
        last_trade_id: usize,
        product_id: String,
        time: DateTime,
    },
    Status {
        products: Vec<StatusProduct>,
        currencies: Vec<StatusCurrency>
    },
    Ticker(Ticker),
    Level2(Level2Book),
    LastMatch(Match),
    Received(Received),
    Open(Open),
    Done(Done),
    Match(Match),
    Activate(Activate),
    Change(Change),
    Error {
        message: String,
    },
    InternalError(crate::CBError), // in futures 0.3 probably TryStream
}

#[derive(Debug)]
pub enum Message {
    Subscriptions {
        channels: Vec<Channel>,
    },
    Heartbeat {
        sequence: usize,
        last_trade_id: usize,
        product_id: String,
        time: DateTime,
    },
    Status {
        products: Vec<StatusProduct>,
        currencies: Vec<StatusCurrency>
    },
    Ticker(Ticker),
    Level2(Level2Book),
    Match(Match),
    Full(Full),
    Error {
        message: String,
    },
    InternalError(crate::CBError), // in futures 0.3 probably TryStream
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    L2Data,
    Snapshot,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub enum OrderSide {
    Bid,
    Offer,
}


#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Level2UpdateRecord {
    pub side: String,
    pub price_level: f64,
    pub new_quantity: f64,
    pub event_time: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Level2Event {
    #[serde(rename = "type")]
    pub _type: String,
    pub product_id: String,
    pub updates: Vec<Level2UpdateRecord>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Level2Book {
    pub channel: String,
    pub client_id: String,
    pub timestamp: String,
    pub sequence_num: u64,
    pub events: Vec<Level2Event>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct StatusProduct {
    pub id: String,
    pub base_currency: String,
    pub quote_currency: String,
    #[serde(deserialize_with = "f64_from_string")]
    pub base_increment: f64,
    #[serde(deserialize_with = "f64_from_string")]
    pub quote_increment: f64,
    pub display_name: String,
    pub status: String,
    pub status_message: String,
    #[serde(deserialize_with = "f64_from_string")]
    pub min_market_funds: f64,
    pub post_only: bool,
    pub limit_only: bool,
    pub cancel_only: bool,
    pub fx_stablecoin: bool
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct StatusCurrency {
    pub id: String,
    pub name: String,
    #[serde(deserialize_with = "f64_from_string")]
    pub min_size: f64,
    pub status: String,
    pub status_message: String,
    #[serde(deserialize_with = "f64_from_string")]
    pub max_precision: f64,
    pub convertible_to: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(untagged)]
#[serde(rename_all = "camelCase")]
pub enum Ticker {
    Full {
        trade_id: usize,
        sequence: usize,
        time: DateTime,
        product_id: String,
        #[serde(deserialize_with = "f64_from_string")]
        price: f64,
        side: super::reqs::OrderSide,
        #[serde(deserialize_with = "f64_from_string")]
        last_size: f64,
        #[serde(deserialize_with = "f64_nan_from_string")]
        best_bid: f64,
        #[serde(deserialize_with = "f64_nan_from_string")]
        best_ask: f64,
    },
    Empty {
        sequence: usize,
        product_id: String,
        #[serde(deserialize_with = "f64_nan_from_string")]
        price: f64,
    },
}

impl Ticker {
    pub fn price(&self) -> &f64 {
        match self {
            Ticker::Full { price, .. } => price,
            Ticker::Empty { price, .. } => price,
        }
    }

    pub fn time(&self) -> Option<&DateTime> {
        match self {
            Ticker::Full { time, .. } => Some(time),
            Ticker::Empty { .. } => None,
        }
    }

    pub fn product_id(&self) -> &str {
        match self {
            Ticker::Full { product_id, .. } => product_id,
            Ticker::Empty { product_id, .. } => product_id,
        }
    }

    pub fn sequence(&self) -> &usize {
        match self {
            Ticker::Full { sequence, .. } => sequence,
            Ticker::Empty { sequence, .. } => sequence,
        }
    }

    pub fn bid(&self) -> Option<&f64> {
        match self {
            Ticker::Full { best_bid, .. } => Some(best_bid),
            Ticker::Empty { .. } => None,
        }
    }

    pub fn ask(&self) -> Option<&f64> {
        match self {
            Ticker::Full { best_ask, .. } => Some(best_ask),
            Ticker::Empty { .. } => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum Full {
    Received(Received),
    Open(Open),
    Done(Done),
    Match(Match),
    Change(Change),
    Activate(Activate),
}

impl Full {
    pub fn price(&self) -> Option<&f64> {
        match self {
            Full::Received(Received::Limit { price, .. }) => Some(price),
            Full::Received(Received::Market { .. }) => None,
            Full::Open(Open { price, .. }) => Some(price),
            Full::Done(Done::Limit { price, .. }) => Some(price),
            Full::Done(Done::Market { .. }) => None,
            Full::Match(Match { price, .. }) => Some(price),
            Full::Change(Change { price, .. }) => price.as_ref(),
            Full::Activate(Activate { .. }) => None,
        }
    }

    pub fn time(&self) -> Option<&DateTime> {
        match self {
            Full::Received(Received::Limit { time, .. }) => Some(time),
            Full::Received(Received::Market { time, .. }) => Some(time),
            Full::Open(Open { time, .. }) => Some(time),
            Full::Done(Done::Limit { time, .. }) => Some(time),
            Full::Done(Done::Market { time, .. }) => Some(time),
            Full::Match(Match { time, .. }) => Some(time),
            Full::Change(Change { time, .. }) => Some(time),
            Full::Activate(Activate { .. }) => None,
        }
    }

    pub fn sequence(&self) -> Option<&usize> {
        match self {
            Full::Received(Received::Limit { sequence, .. }) => Some(sequence),
            Full::Received(Received::Market { sequence, .. }) => Some(sequence),
            Full::Open(Open { sequence, .. }) => Some(sequence),
            Full::Done(Done::Limit { sequence, .. }) => sequence.as_ref(),
            Full::Done(Done::Market { sequence, .. }) => Some(sequence),
            Full::Match(Match { sequence, .. }) => Some(sequence),
            Full::Change(Change { sequence, .. }) => Some(sequence),
            Full::Activate(Activate { .. }) => None,
        }
    }

    pub fn product_id(&self) -> &str {
        match self {
            Full::Received(Received::Limit { product_id, .. }) => product_id,
            Full::Received(Received::Market { product_id, .. }) => product_id,
            Full::Open(Open { product_id, .. }) => product_id,
            Full::Done(Done::Limit { product_id, .. }) => product_id,
            Full::Done(Done::Market { product_id, .. }) => product_id,
            Full::Match(Match { product_id, .. }) => product_id,
            Full::Change(Change { product_id, .. }) => product_id,
            Full::Activate(Activate { product_id, .. }) => product_id,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(tag = "order_type")]
#[serde(rename_all = "camelCase")]
pub enum Received {
    Limit {
        time: DateTime,
        product_id: String,
        sequence: usize,
        order_id: Uuid,
        #[serde(deserialize_with = "uuid_opt_from_string")]
        client_oid: Option<Uuid>,
        #[serde(deserialize_with = "f64_from_string")]
        size: f64,
        #[serde(deserialize_with = "f64_from_string")]
        price: f64,
        side: super::reqs::OrderSide,
        user_id: Option<String>,
        #[serde(default)]
        #[serde(deserialize_with = "uuid_opt_from_string")]
        profile_id: Option<Uuid>,
    },
    Market {
        time: DateTime,
        product_id: String,
        sequence: usize,
        order_id: Uuid,
        #[serde(deserialize_with = "uuid_opt_from_string")]
        client_oid: Option<Uuid>,
        #[serde(default)]
        #[serde(deserialize_with = "f64_opt_from_string")]
        funds: Option<f64>,
        side: super::reqs::OrderSide,
    },
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Open {
    pub time: DateTime,
    pub product_id: String,
    pub sequence: usize,
    pub order_id: Uuid,
    #[serde(deserialize_with = "f64_from_string")]
    pub price: f64,
    #[serde(deserialize_with = "f64_from_string")]
    pub remaining_size: f64,
    pub side: super::reqs::OrderSide,
    pub user_id: Option<String>,
    #[serde(default)]
    #[serde(deserialize_with = "uuid_opt_from_string")]
    pub profile_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(untagged)]
pub enum Done {
    Limit {
        time: DateTime,
        product_id: String,
        sequence: Option<usize>,
        #[serde(deserialize_with = "f64_from_string")]
        price: f64,
        order_id: Uuid,
        reason: Reason,
        side: super::reqs::OrderSide,
        #[serde(deserialize_with = "f64_from_string")]
        remaining_size: f64,
        user_id: Option<String>,
        #[serde(default)]
        #[serde(deserialize_with = "uuid_opt_from_string")]
        profile_id: Option<Uuid>,
    },
    Market {
        time: DateTime,
        product_id: String,
        sequence: usize,
        order_id: Uuid,
        reason: Reason,
        side: super::reqs::OrderSide,
    },
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Reason {
    Filled,
    Canceled,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Match {
    pub trade_id: usize,
    pub sequence: usize,
    pub maker_order_id: Uuid,
    pub taker_order_id: Uuid,
    pub time: DateTime,
    pub product_id: String,
    #[serde(deserialize_with = "f64_from_string")]
    pub size: f64,
    #[serde(deserialize_with = "f64_from_string")]
    pub price: f64,
    pub side: super::reqs::OrderSide,
    pub taker_user_id: Option<String>,
    pub taker_profile_id: Option<Uuid>,
    #[serde(default)]
    #[serde(deserialize_with = "f64_opt_from_string")]
    pub taker_fee_rate: Option<f64>,

    pub maker_user_id: Option<String>,
    pub maker_profile_id: Option<Uuid>,
    #[serde(default)]
    #[serde(deserialize_with = "f64_opt_from_string")]
    pub maker_fee_rate: Option<f64>,

    pub user_id: Option<String>,
    #[serde(default)]
    #[serde(deserialize_with = "uuid_opt_from_string")]
    pub profile_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Change {
    pub time: DateTime,
    pub sequence: usize,
    pub order_id: Uuid,
    pub product_id: String,
    #[serde(deserialize_with = "f64_from_string")]
    pub new_size: f64,
    #[serde(deserialize_with = "f64_from_string")]
    pub old_size: f64,
    #[serde(default)]
    #[serde(deserialize_with = "f64_opt_from_string")]
    pub new_funds: Option<f64>,
    #[serde(default)]
    #[serde(deserialize_with = "f64_opt_from_string")]
    pub old_funds: Option<f64>,
    #[serde(default)]
    #[serde(deserialize_with = "f64_opt_from_string")]
    pub price: Option<f64>,
    pub side: super::reqs::OrderSide,
    pub user_id: Option<String>,
    #[serde(default)]
    #[serde(deserialize_with = "uuid_opt_from_string")]
    pub profile_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Activate {
    pub product_id: String,
    #[serde(deserialize_with = "f64_from_string")]
    pub timestamp: f64,
    pub order_id: Uuid,
    pub stop_type: StopType,
    #[serde(deserialize_with = "f64_from_string")]
    pub size: f64,
    #[serde(deserialize_with = "f64_from_string")]
    pub funds: f64,
    #[serde(deserialize_with = "f64_from_string")]
    pub taker_fee_rate: f64,
    pub private: bool,
    pub user_id: Option<String>,
    #[serde(default)]
    #[serde(deserialize_with = "uuid_opt_from_string")]
    pub profile_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum StopType {
    Entry,
    Exit,
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
    {
        let json_value = Value::deserialize(deserializer)?;

        if let Ok(ticker) = serde_json::from_value::<Ticker>(json_value.clone()) {
            return Ok(Message::Ticker(ticker));
        }

        if let Ok(level2) = serde_json::from_value::<Level2Book>(json_value.clone()) {
            return Ok(Message::Level2(level2));
        }


        // If none of the deserializations were successful, return an error
        Err(de::Error::custom("Unknown message type"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::str::FromStr;

    #[test]
    fn test_parse_numbers() {
        #[derive(Serialize, Deserialize, Debug)]
        struct S {
            #[serde(deserialize_with = "f64_from_string")]
            a: f64,
            #[serde(deserialize_with = "f64_from_string")]
            b: f64,
            #[serde(deserialize_with = "f64_nan_from_string")]
            c: f64,
            #[serde(deserialize_with = "f64_opt_from_string")]
            d: Option<f64>,
            #[serde(deserialize_with = "f64_opt_from_string")]
            e: Option<f64>,
            #[serde(deserialize_with = "f64_opt_from_string")]
            f: Option<f64>,
            #[serde(default)]
            #[serde(deserialize_with = "f64_opt_from_string")]
            j: Option<f64>,
        }

        let json = r#"{
            "a": 5.5,
            "b":"5.5",
            "c":"",
            "d":"5.6",
            "e":5.6,
            "f":""
            }"#;
        let s: S = serde_json::from_str(json).unwrap();

        assert_eq!(5.5, s.a);
        assert_eq!(5.5, s.b);
        assert!(s.c.is_nan());
        assert_eq!(Some(5.6), s.d);
        assert_eq!(Some(5.6), s.e);
        assert_eq!(None, s.f);
        assert_eq!(None, s.j);
    }

    #[test]
    fn test_change_without_price() {
        let json = r#"{ "type" : "change", "side" : "sell", "old_size" : "7.53424298",
            "new_size" : "4.95057246", "order_id" : "0f352cbb-98a8-48ce-9dc6-3003870dcfd1",
            "product_id" : "BTC-USD", "sequence" : 7053090065,
            "time" : "2018-09-25T13:30:57.550000Z" }"#;

        let m: Message = serde_json::from_str(json).unwrap();
        let str = format!("{:?}", m);
        assert!(str.contains("product_id: \"BTC-USD\""));
    }

    #[test]
    fn test_canceled_order_done() {
        let json = r#"{"type": "done", "side": "sell", "order_id": "d05c295b-af2e-4f5e-bfa0-55d93370c450",
                       "reason":"canceled","product_id":"BTC-USD","price":"10009.17000000","remaining_size":"0.00973768",
                       "user_id":"0fd194ab8a8bf175a75f8de5","profile_id":"fa94ac51-b20a-4b16-bc7a-af3c0abb7ec4",
                       "time":"2019-08-21T22:10:15.190000Z"}"#;
        let m: Message = serde_json::from_str(json).unwrap();
        let str = format!("{:?}", m);
        assert!(str.contains("product_id: \"BTC-USD\""));
        assert!(str.contains("user_id: Some"));
        assert!(str.contains("profile_id: Some"));
    }

    #[test]
    fn test_canceled_order_without_auth() {
        let json = r#"{"type": "done", "side": "sell", "order_id": "d05c295b-af2e-4f5e-bfa0-55d93370c450",
                       "reason":"canceled","product_id":"BTC-USD","price":"10009.17000000","remaining_size":"0.00973768",
                       "time":"2019-08-21T22:10:15.190000Z"}"#;
        let m: Message = serde_json::from_str(json).unwrap();
        let str = format!("{:?}", m);
        assert!(str.contains("product_id: \"BTC-USD\""));
        assert!(str.contains("user_id: None"));
        assert!(str.contains("profile_id: None"));
    }

    #[test]
    fn test_parse_uuid() {
        #[derive(Serialize, Deserialize, Debug)]
        struct S {
            #[serde(deserialize_with = "uuid_opt_from_string")]
            uuid: Option<Uuid>,
        }

        let json = r#"{
            "uuid":"2fec40ac-525b-4192-871a-39d784945055"
            }"#;
        let s: S = serde_json::from_str(json).unwrap();

        assert_eq!(
            s.uuid,
            Some(Uuid::from_str("2fec40ac-525b-4192-871a-39d784945055").unwrap())
        );

        let json = r#"{
            "uuid":""
            }"#;
        let s: S = serde_json::from_str(json).unwrap();

        assert!(s.uuid.is_none());
    }
}
