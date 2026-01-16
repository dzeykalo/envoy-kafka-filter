use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};
use std::time::Duration;

pub trait Producer: Send + Sync {
    fn new(servers: &str) -> Self where Self: Sized;
    fn connect(&mut self) -> Result<(), String>;
    fn produce(&mut self, topic: &str, payload: &[u8]) -> Result<(), String>;
}
pub struct KafkaProducer {
    bootstrap_servers: String,
    producer:  Option<BaseProducer>
}

impl Producer for KafkaProducer {
    fn new(servers: &str) -> Self {
        Self {
            bootstrap_servers: servers.to_string(),
            producer: None
        }
    }

    fn connect(&mut self) -> Result<(), String> {
        match &self.producer {
            Some(_) => Ok(()),
            None => {
                let producer =  ClientConfig::new()
                    .set("bootstrap.servers", &self.bootstrap_servers)
                    .create();
                match producer {
                    Ok(p) => {
                        self.producer = Some(p);
                        Ok(())
                    }
                    Err(e) => Err(format!("Failed to create producer: {}", e))
                }
            }
        }
    }

    fn produce(&mut self, topic: &str, payload: &[u8]) -> Result<(), String> {
        if self.producer.is_none() {
            match self.connect() {
                Ok(_) => {},
                Err(e) => return Err(e)
            }
        }
        let producer = match &self.producer {
            Some(p) => p,
            None => return Err("Producer not initialized".to_string())
        };

        let delivery_status = producer.send(
            BaseRecord::to(topic)
                .payload(payload)
                .key(""),
        );

        match delivery_status {
            Ok(_) => {
                for _ in 0..10 {
                    let _ = producer.poll(Duration::from_millis(100));
                }
            }
            Err((e, _)) => {
                return Err(format!("Failed to send message: {}", e));
            }
        }

        let record = BaseRecord::to(topic).payload(payload).key("");
        match producer.send(record) {
            Ok(()) => {
                for _ in 0..10 {
                    producer.poll(Duration::from_millis(100));
                }
                Ok(())
            }
            Err((e, _)) => Err(format!("Failed to send message: {}", e)),
        }
    }
}