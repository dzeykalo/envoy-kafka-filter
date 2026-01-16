use std::time::Duration;
use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::Consumer as RdKafkaConsumer;
use rdkafka::config::ClientConfig;
use rdkafka::Message;

pub trait Consumer: Send + Sync {
    fn new(servers: &str) -> Self where Self: Sized;
    fn connect(&mut self) -> Result<(), String>;
    fn consume(&mut self, topic: &str, timeout: Duration) -> Result<Option<Vec<u8>>, String>;
}
pub struct KafkaConsumer {
    bootstrap_servers: String,
    consumer:  Option<BaseConsumer>
}

impl Consumer for KafkaConsumer {
    fn new(servers: &str) -> Self {
        KafkaConsumer {
            bootstrap_servers: servers.to_string(),
            consumer: None
        }
    }
    fn connect(&mut self) -> Result<(), String> {
        match &self.consumer {
            Some(_) => Ok(()),
            None => {
                let consumer = ClientConfig::new()
                    .set("bootstrap.servers", &self.bootstrap_servers)
                    .set("group.id", "http-proxy-group")
                    .set("session.timeout.ms", "6000")
                    .set("auto.offset.reset", "earliest")
                    .create();
                match consumer {
                    Ok(c) => {
                        self.consumer = Some(c);
                        Ok(())
                    },
                    Err(e) => Err(format!("Failed to create consumer: {}", e))
                }
            }
        }
    }

    fn consume(&mut self, topic: &str, timeout: Duration) -> Result<Option<Vec<u8>>, String> {
        if self.consumer.is_none() {
            match self.connect() {
                Ok(_) => {},
                Err(e) => return Err(e)
            }
        }

        let consumer = match &self.consumer {
            Some(p) => p,
            None => return Err("Consumer not initialized".to_string())
        };
        let topics = vec![topic];
        if let Err(e) = consumer.subscribe(&topics) {
            return Err(format!("Failed to subscribe to topics: {}", e));
        }

        loop {
            match consumer.poll(timeout) {
                Some(Ok(message)) => {
                    if let Some(payload) = message.payload() {
                        return Ok(Some(payload.to_vec()));
                    }
                },
                Some(Err(e)) => {
                    return Err(format!("Error receiving message: {}", e));
                },
                None => continue
            }
        }
    }
}