use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer as RdKafkaConsumer};
use rdkafka::Message;
use std::time::Duration;

pub struct KafkaConsumerClient {
    bootstrap_servers: String,
    group_id: String,
    consumer: Option<BaseConsumer>,
    subscribed_topic: Option<String>,
}

impl KafkaConsumerClient {
    pub fn new(bootstrap_servers: String, group_id: String) -> Self {
        Self {
            bootstrap_servers,
            group_id,
            consumer: None,
            subscribed_topic: None,
        }
    }

    pub fn connect(&mut self) -> Result<(), String> {
        if self.consumer.is_some() {
            return Ok(());
        }

        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", &self.group_id)
            .set("session.timeout.ms", "6000")
            .set("auto.offset.reset", "latest")
            .create()
            .map_err(|e| format!("Failed to create consumer: {}", e))?;

        self.consumer = Some(consumer);
        Ok(())
    }

    pub fn subscribe(&mut self, topic: &str) -> Result<(), String> {
        if self.consumer.is_none() {
            self.connect()?;
        }

        if self.subscribed_topic.as_deref() == Some(topic) {
            return Ok(());
        }

        let consumer = self
            .consumer
            .as_ref()
            .ok_or("Consumer not initialized".to_string())?;
        consumer
            .subscribe(&[topic])
            .map_err(|e| format!("Failed to subscribe: {}", e))?;

        self.subscribed_topic = Some(topic.to_string());
        Ok(())
    }

    pub fn poll(&self, timeout: Duration) -> Result<Option<Vec<u8>>, String> {
        let consumer = self
            .consumer
            .as_ref()
            .ok_or("Consumer not initialized".to_string())?;

        match consumer.poll(timeout) {
            Some(Ok(message)) => Ok(message.payload().map(|p| p.to_vec())),
            Some(Err(e)) => Err(format!("Error receiving message: {}", e)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    use std::env;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn bootstrap_servers() -> String {
        env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string())
    }

    fn test_topic() -> String {
        env::var("KAFKA_TEST_TOPIC").unwrap_or_else(|_| "topic0".to_string())
    }

    fn unique_suffix() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    }

    fn send_test_message(bootstrap: &str, topic: &str, payload: &[u8]) -> Result<(), String> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("allow.auto.create.topics", "true")
            .create::<BaseProducer>()
            .map_err(|e| format!("Failed to create producer: {}", e))?;

        producer
            .send(BaseRecord::to(topic).payload(payload).key(""))
            .map_err(|(e, _)| format!("Failed to send message: {}", e))?;

        for _ in 0..10 {
            producer.poll(Duration::from_millis(100));
        }

        let _ = producer.flush(Duration::from_secs(1));

        Ok(())
    }

    #[test]
    fn test_poll_receive() {
        let bootstrap = bootstrap_servers();
        let topic = test_topic();
        let payload = format!("test-message-{}", unique_suffix()).into_bytes();

        let topic_clone = topic.clone();
        let payload_clone = payload.clone();
        let alive = Arc::new(AtomicBool::new(true));
        let alive_clone = Arc::clone(&alive);
        let producer_handle = std::thread::spawn(move || {
            while alive_clone.load(Ordering::Relaxed) {
                send_test_message(&bootstrap, &topic_clone, &payload_clone)
                    .expect("producer send failed");
                std::thread::sleep(Duration::from_millis(500));
            }
        });

        let mut client = KafkaConsumerClient::new(
            bootstrap_servers(),
            format!("test-group-{}", unique_suffix()),
        );
        client.connect().expect("connect failed");
        client.subscribe(&topic).expect("subscribe failed");

        let mut received = None;
        for _ in 0..20 {
            match client.poll(Duration::from_millis(500)) {
                Ok(Some(bytes)) => {
                    println!("received from kafka: {}", String::from_utf8_lossy(&bytes));
                    received = Some(bytes);
                    break;
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                Err(err) => panic!("poll failed: {}", err),
            }
        }

        alive.store(false, Ordering::Relaxed);
        producer_handle.join().expect("producer thread failed");
        assert_eq!(received.as_deref(), Some(payload.as_slice()));
    }
}
