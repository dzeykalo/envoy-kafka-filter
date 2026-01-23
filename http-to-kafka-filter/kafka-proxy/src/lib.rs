mod consumer;
mod producer;

use std::collections::HashMap;
use std::slice;
use std::sync::Mutex;
use std::time::Duration;

use consumer::KafkaConsumerClient;
use lazy_static::lazy_static;
use producer::KafkaProducerClient;

struct ConsumerEntry {
    group_id: String,
    client: KafkaConsumerClient,
}

struct GlobalState {
    bootstrap_servers: Option<String>,
    producer: Option<KafkaProducerClient>,

    consumers: HashMap<u64, ConsumerEntry>,
    next_client_id: u64,
    topic_counters: HashMap<String, u64>,

    last_messages: HashMap<u64, Box<[u8]>>,
    last_error: Option<Box<[u8]>>,
}

impl GlobalState {
    fn set_error(&mut self, msg: String) {
        self.last_error = Some(msg.into_bytes().into_boxed_slice());
    }
}

lazy_static! {
    static ref GLOBAL: Mutex<GlobalState> = Mutex::new(GlobalState {
        bootstrap_servers: None,
        producer: None,
        consumers: HashMap::new(),
        next_client_id: 1,
        topic_counters: HashMap::new(),
        last_messages: HashMap::new(),
        last_error: None,
    });
}

fn get_bootstrap(gs: &GlobalState) -> Result<String, String> {
    gs.bootstrap_servers
        .clone()
        .ok_or_else(|| "bootstrap.servers not set".to_string())
}

#[unsafe(no_mangle)]
/// # Safety
/// `ptr` must be valid for reads of `len` bytes and remain valid for the
/// duration of this call.
pub unsafe extern "C" fn set_bootstrap_servers(ptr: *const u8, len: u64) -> u64 {
    if ptr.is_null() || len == 0 {
        return 500;
    }
    let s = unsafe { slice::from_raw_parts(ptr, len as usize) };
    let v = String::from_utf8_lossy(s).to_string();

    let mut gs = GLOBAL.lock().unwrap();
    gs.bootstrap_servers = Some(v);
    200
}

#[unsafe(no_mangle)]
/// # Safety
/// `topic_ptr` must be valid for reads of `topic_len` bytes and remain valid for
/// the duration of this call.
pub unsafe extern "C" fn create_consumer(topic_ptr: *const u8, topic_len: u64) -> u64 {
    if topic_ptr.is_null() || topic_len == 0 {
        return 0;
    }
    let topic_bytes = unsafe { slice::from_raw_parts(topic_ptr, topic_len as usize) };
    let topic = String::from_utf8_lossy(topic_bytes).to_string();

    let mut gs = GLOBAL.lock().unwrap();
    let bootstrap = match get_bootstrap(&gs) {
        Ok(v) => v,
        Err(e) => {
            gs.set_error(e);
            return 0;
        }
    };

    let counter = gs.topic_counters.entry(topic.clone()).or_insert(0);
    *counter += 1;

    let group_id = format!("{}-group-{}", topic, *counter);
    let mut client = KafkaConsumerClient::new(bootstrap, group_id.clone());

    if let Err(e) = client.connect().and_then(|_| client.subscribe(&topic)) {
        gs.set_error(e);
        return 0;
    }

    let id = gs.next_client_id;
    gs.next_client_id += 1;

    gs.consumers.insert(id, ConsumerEntry { group_id, client });

    id
}

#[unsafe(no_mangle)]
pub extern "C" fn destroy_consumer(client_id: u64) {
    let mut gs = GLOBAL.lock().unwrap();
    gs.consumers.remove(&client_id);
    gs.last_messages.remove(&client_id);
}

#[unsafe(no_mangle)]
pub extern "C" fn consumer_group_id_ptr(client_id: u64) -> *const u8 {
    let gs = GLOBAL.lock().unwrap();
    match gs.consumers.get(&client_id) {
        Some(entry) => entry.group_id.as_ptr(),
        None => std::ptr::null(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn consumer_group_id_len(client_id: u64) -> u64 {
    let gs = GLOBAL.lock().unwrap();
    match gs.consumers.get(&client_id) {
        Some(entry) => entry.group_id.len() as u64,
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn poll_message(client_id: u64, timeout_ms: u64) -> u64 {
    let mut gs = GLOBAL.lock().unwrap();
    let entry = match gs.consumers.get(&client_id) {
        Some(e) => e,
        None => {
            gs.set_error("Consumer not found".to_string());
            return 404;
        }
    };

    match entry.client.poll(Duration::from_millis(timeout_ms)) {
        Ok(Some(payload)) => {
            gs.last_messages
                .insert(client_id, payload.into_boxed_slice());
            200
        }
        Ok(None) => 0, // timeout
        Err(e) => {
            gs.set_error(e);
            500
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn last_message_ptr(client_id: u64) -> *const u8 {
    let gs = GLOBAL.lock().unwrap();
    match gs.last_messages.get(&client_id) {
        Some(buf) => buf.as_ptr(),
        None => std::ptr::null(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn last_message_len(client_id: u64) -> u64 {
    let gs = GLOBAL.lock().unwrap();
    match gs.last_messages.get(&client_id) {
        Some(buf) => buf.len() as u64,
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn clear_last_message(client_id: u64) {
    let mut gs = GLOBAL.lock().unwrap();
    gs.last_messages.remove(&client_id);
}

#[unsafe(no_mangle)]
pub extern "C" fn last_error_ptr() -> *const u8 {
    let gs = GLOBAL.lock().unwrap();
    match gs.last_error.as_ref() {
        Some(buf) => buf.as_ptr(),
        None => std::ptr::null(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn last_error_len() -> u64 {
    let gs = GLOBAL.lock().unwrap();
    match gs.last_error.as_ref() {
        Some(buf) => buf.len() as u64,
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn clear_last_error() {
    let mut gs = GLOBAL.lock().unwrap();
    gs.last_error = None;
}

#[unsafe(no_mangle)]
pub extern "C" fn ensure_producer() -> u64 {
    let mut gs = GLOBAL.lock().unwrap();
    if gs.producer.is_some() {
        return 200;
    }

    let bootstrap = match get_bootstrap(&gs) {
        Ok(v) => v,
        Err(e) => {
            gs.set_error(e);
            return 500;
        }
    };

    let mut producer = KafkaProducerClient::new(bootstrap);
    match producer.connect() {
        Ok(_) => {
            gs.producer = Some(producer);
            200
        }
        Err(e) => {
            gs.set_error(e);
            500
        }
    }
}

#[unsafe(no_mangle)]
/// # Safety
/// `topic_ptr` must be valid for reads of `topic_len` bytes and `payload_ptr`
/// must be valid for reads of `payload_len` bytes for the duration of this call.
pub unsafe extern "C" fn produce_message(
    topic_ptr: *const u8,
    topic_len: u64,
    payload_ptr: *const u8,
    payload_len: u64,
) -> u64 {
    if topic_ptr.is_null() || payload_ptr.is_null() || topic_len == 0 || payload_len == 0 {
        return 500;
    }

    let topic_bytes = unsafe { slice::from_raw_parts(topic_ptr, topic_len as usize) };
    let payload = unsafe { slice::from_raw_parts(payload_ptr, payload_len as usize) };
    let topic = String::from_utf8_lossy(topic_bytes).to_string();

    let mut gs = GLOBAL.lock().unwrap();
    if gs.producer.is_none() {
        let bootstrap = match get_bootstrap(&gs) {
            Ok(v) => v,
            Err(e) => {
                gs.set_error(e);
                return 500;
            }
        };
        gs.producer = Some(KafkaProducerClient::new(bootstrap));
    }

    let producer = gs.producer.as_mut().unwrap();
    match producer.produce(&topic, payload) {
        Ok(_) => 200,
        Err(e) => {
            gs.set_error(e);
            500
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn producer_poll(timeout_ms: u64) -> u64 {
    let mut gs = GLOBAL.lock().unwrap();
    let producer = match gs.producer.as_ref() {
        Some(p) => p,
        None => {
            gs.set_error("Producer not initialized".to_string());
            return 500;
        }
    };

    match producer.poll(Duration::from_millis(timeout_ms)) {
        Ok(_) => 200,
        Err(e) => {
            gs.set_error(e);
            500
        }
    }
}

#[unsafe(no_mangle)]
/// # Safety
/// `ptr` must have been allocated by Rust as a boxed slice (`Box<[u8]>`) with
/// length `len` and must not be used after this call.
pub unsafe extern "C" fn free_buffer(ptr: *mut u8, len: u64) {
    if !ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len as usize));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::ffi::CString;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use std::thread::{self, sleep};
    use std::time::Duration;

    fn bootstrap_servers() -> String {
        env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string())
    }

    fn test_topic() -> String {
        env::var("KAFKA_TEST_TOPIC").unwrap_or_else(|_| "topic0".to_string())
    }

    fn cstr(s: &str) -> (CString, *const u8, u64) {
        let c_str = CString::new(s).unwrap();
        let ptr = c_str.as_ptr() as *const u8;
        let len = s.len() as u64;
        (c_str, ptr, len)
    }

    fn last_error_string() -> String {
        let ptr = last_error_ptr();
        let len = last_error_len();
        let msg = if !ptr.is_null() && len > 0 {
            let bytes = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
            String::from_utf8_lossy(bytes).to_string()
        } else {
            String::new()
        };
        clear_last_error();
        msg
    }

    #[test]
    fn test_producer_send_message() {
        let bootstrap = bootstrap_servers();
        let topic = test_topic();

        let (_c_bootstrap, bootstrap_ptr, bootstrap_len) = cstr(&bootstrap);
        assert_eq!(
            unsafe { set_bootstrap_servers(bootstrap_ptr, bootstrap_len) },
            200
        );

        let status = ensure_producer();
        assert_eq!(
            status,
            200,
            "ensure_producer failed: {}",
            last_error_string()
        );

        let (_c_topic, topic_ptr, topic_len) = cstr(&topic);
        let (_c_payload, payload_ptr, payload_len) = cstr("test-message");

        let status = unsafe { produce_message(topic_ptr, topic_len, payload_ptr, payload_len) };
        assert_eq!(
            status,
            200,
            "produce_message failed: {}",
            last_error_string()
        );

        assert_eq!(
            producer_poll(200),
            200,
            "producer_poll failed: {}",
            last_error_string()
        );
    }

    #[test]
    fn test_consumer_receive() {
        let bootstrap = bootstrap_servers();
        let topic = test_topic();

        let (_c_bootstrap, bootstrap_ptr, bootstrap_len) = cstr(&bootstrap);
        assert_eq!(
            unsafe { set_bootstrap_servers(bootstrap_ptr, bootstrap_len) },
            200
        );

        let (_c_topic, topic_ptr, topic_len) = cstr(&topic);

        let client_id = unsafe { create_consumer(topic_ptr, topic_len) };
        assert!(
            client_id > 0,
            "create_consumer failed: {}",
            last_error_string()
        );

        let alive = Arc::new(AtomicBool::new(true));
        let alive_clone = Arc::clone(&alive);
        let producer_topic = topic.clone();
        let producer_handle = thread::spawn(move || {
            while alive_clone.load(Ordering::Relaxed) {
                let (_c_topic, topic_ptr, topic_len) = cstr(&producer_topic);
                let (_c_payload, payload_ptr, payload_len) = cstr("hello-consumer");
                let status =
                    unsafe { produce_message(topic_ptr, topic_len, payload_ptr, payload_len) };
                assert_eq!(
                    status,
                    200,
                    "produce_message failed: {}",
                    last_error_string()
                );
                sleep(Duration::from_millis(200));
            }
        });

        let mut received = None;
        for _ in 0..20 {
            let status = poll_message(client_id, 200);
            if status == 200 {
                let msg_ptr = last_message_ptr(client_id);
                let msg_len = last_message_len(client_id);
                if !msg_ptr.is_null() && msg_len > 0 {
                    let bytes = unsafe { std::slice::from_raw_parts(msg_ptr, msg_len as usize) };
                    received = Some(String::from_utf8_lossy(bytes).to_string());
                }
                clear_last_message(client_id);
                break;
            } else if status == 0 {
                sleep(Duration::from_millis(100));
            } else {
                panic!("poll_message failed: {}", last_error_string());
            }
        }

        alive.store(false, Ordering::Relaxed);
        producer_handle.join().expect("producer thread failed");
        destroy_consumer(client_id);
        assert_eq!(received.as_deref(), Some("hello-consumer"));
    }
}
