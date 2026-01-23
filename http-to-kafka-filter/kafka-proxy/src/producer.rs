use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};
use std::time::Duration;

pub struct KafkaProducerClient {
    bootstrap_servers: String,
    producer: Option<BaseProducer>,
}

impl KafkaProducerClient {
    pub fn new(bootstrap_servers: String) -> Self {
        Self {
            bootstrap_servers,
            producer: None,
        }
    }

    pub fn connect(&mut self) -> Result<(), String> {
        if self.producer.is_some() {
            return Ok(());
        }

        let producer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("allow.auto.create.topics", "true")
            .create()
            .map_err(|e| format!("Failed to create producer: {}", e))?;

        self.producer = Some(producer);
        Ok(())
    }

    pub fn produce(&mut self, topic: &str, payload: &[u8]) -> Result<(), String> {
        if self.producer.is_none() {
            self.connect()?;
        }

        let producer = self
            .producer
            .as_ref()
            .ok_or("Producer not initialized".to_string())?;

        producer
            .send(BaseRecord::to(topic).payload(payload).key(""))
            .map_err(|(e, _)| format!("Failed to send message: {}", e))?;

        for _ in 0..5 {
            producer.poll(Duration::from_millis(50));
        }

        Ok(())
    }

    pub fn poll(&self, timeout: Duration) -> Result<(), String> {
        let producer = self
            .producer
            .as_ref()
            .ok_or("Producer not initialized".to_string())?;
        producer.poll(timeout);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn bootstrap_servers() -> String {
        env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string())
    }

    fn test_topic() -> String {
        env::var("KAFKA_TEST_TOPIC").unwrap_or_else(|_| "test0".to_string())
    }

    fn unique_suffix() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    }

    #[test]
    fn test_producer_send() {
        let bootstrap = bootstrap_servers();
        let topic = test_topic();

        let mut producer = KafkaProducerClient::new(bootstrap);
        producer.connect().expect("connect failed");

        let payload = format!("producer-test-{}", unique_suffix()).into_bytes();
        producer.produce(&topic, &payload).expect("produce failed");

        producer
            .poll(Duration::from_millis(200))
            .expect("poll failed");
    }
}
