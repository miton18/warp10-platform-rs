//! Kafka producer abstraction.
//!
//! Default: an in-memory mock producer (records every message, used by tests
//! and local runs). With the `kafka` cargo feature, a real librdkafka producer.
//!
//! Wire contract (from Ingress.java, distributed mode):
//! - data topic: key = 16 bytes classId(BE)||labelsId(BE), value = thrift
//!   compact `KafkaDataMessage` (STORE: classId, labelsId, data=encoder bytes,
//!   type=1). AES wrap / SipHash MAC of the value are applied only when the
//!   platform keys are configured (out of scope here).
//! - meta topic: key = same 16 bytes, value = key bytes ++ thrift compact
//!   `Metadata` (name, labels incl. .owner/.producer/.app, classId, labelsId).

use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub topic: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub trait Producer: Send + Sync {
    fn send(&self, topic: &str, key: &[u8], value: &[u8]) -> std::io::Result<()>;
    fn flush(&self) -> std::io::Result<()>;
}

/// In-memory producer: keeps every record, also handy for assertions.
#[derive(Clone, Default)]
pub struct MockProducer {
    pub records: Arc<Mutex<Vec<Record>>>,
}

impl MockProducer {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn records(&self) -> Vec<Record> {
        self.records.lock().unwrap().clone()
    }
}

impl Producer for MockProducer {
    fn send(&self, topic: &str, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        self.records.lock().unwrap().push(Record {
            topic: topic.to_string(),
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    }
    fn flush(&self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "kafka")]
pub struct RdkafkaProducer {
    producer: rdkafka::producer::FutureProducer,
}

#[cfg(feature = "kafka")]
impl RdkafkaProducer {
    pub fn new(brokers: &str) -> Result<Self, String> {
        use rdkafka::config::ClientConfig;
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|e| e.to_string())?;
        Ok(RdkafkaProducer { producer })
    }
}

#[cfg(feature = "kafka")]
impl Producer for RdkafkaProducer {
    fn send(&self, topic: &str, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        use rdkafka::producer::FutureRecord;
        let rec = FutureRecord::to(topic).key(key).payload(value);
        let _ = self
            .producer
            .send(rec, std::time::Duration::from_secs(5))
            .map_err(|(e, _)| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        Ok(())
    }
    fn flush(&self) -> std::io::Result<()> {
        self.producer.flush(std::time::Duration::from_secs(10)).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, e)
        })
    }
}
