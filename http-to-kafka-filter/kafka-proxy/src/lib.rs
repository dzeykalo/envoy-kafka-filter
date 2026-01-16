mod producer;
mod consumer;

use std::slice;
use std::sync::Mutex;
use std::time::Duration;
use lazy_static::lazy_static;
use producer::{KafkaProducer, Producer};
use consumer::{KafkaConsumer, Consumer};

lazy_static! {
    static ref PRODUCER: Mutex<Option<Box<dyn Producer + Send>>> = Mutex::new(None);
    static ref CONSUMER: Mutex<Option<Box<dyn Consumer + Send>>> = Mutex::new(None);
}

#[unsafe(no_mangle)]
pub extern "C" fn set_config(bootstraps: *const u8, bootstraps_len: u64) -> u64 {
    if bootstraps.is_null() {
        return 500;
    }

    let bootstraps_slice = unsafe {
        slice::from_raw_parts(bootstraps, bootstraps_len as usize)
    };
    let bootstrap_servers = String::from_utf8_lossy(bootstraps_slice);

    let mut producer_guard = PRODUCER.lock().unwrap();
    let mut consumer_guard = CONSUMER.lock().unwrap();
    if producer_guard.is_some() && consumer_guard.is_some() {
        return 200;
    }

    let producer = KafkaProducer::new(&bootstrap_servers);
    let producer_box: Box<dyn Producer + Send> = Box::new(producer);
    *producer_guard = Some(producer_box);

    let consumer = KafkaConsumer::new(&bootstrap_servers);
    let consumer_box: Box<dyn Consumer + Send> = Box::new(consumer);
    *consumer_guard = Some(consumer_box);

    200
}

#[unsafe(no_mangle)]
pub extern "C" fn produce(topic_name: *const u8, topic_name_len: u64,
                          payload: *const u8, payload_len: u64,
                          err_msg_ptr: *mut *mut u8, err_msg_len: *mut u64) -> u64 {
    if topic_name.is_null() || payload.is_null() || topic_name_len == 0 || payload_len == 0 {
        if !err_msg_len.is_null() {
            unsafe { *err_msg_len = 0; }
        }
        return 500;
    }

    let topic_name_slice = unsafe {
        slice::from_raw_parts(topic_name, topic_name_len as usize)
    };
    let topic_name = String::from_utf8_lossy(topic_name_slice);
    print!("topic name {}", topic_name);

    let payload_slice = unsafe {
        slice::from_raw_parts(payload, payload_len as usize)
    };

    let mut producer_guard = PRODUCER.lock().unwrap();
    if let Some(ref mut producer) = *producer_guard {
        match producer.produce(&topic_name, payload_slice) {
            Ok(()) => return 200,
            Err(err_msg) => {
                let err_vec = err_msg.into_bytes();
                let len = err_vec.len() as u64;
                let boxed = err_vec.into_boxed_slice();
                let ptr = Box::into_raw(boxed) as *mut u8;
                unsafe {
                    *err_msg_ptr = ptr;
                    *err_msg_len = len;
                }
            }
        }
    }
    500 // Internal Server Error
}

#[unsafe(no_mangle)]
pub extern "C" fn consume(topic_name: *const u8, topic_name_len: u64,
                                  out_ptr: *mut *mut u8, out_len: *mut u64,
                                  err_msg_ptr: *mut *mut u8, err_msg_len: *mut u64) -> u64 {
    if topic_name.is_null() || out_len.is_null() || err_msg_len.is_null() || topic_name_len == 0 {
        if !err_msg_len.is_null() {
            unsafe { *err_msg_len = 0; }
        }
        return 500;
    }

    let topic_name_slice = unsafe {
        slice::from_raw_parts(topic_name, topic_name_len as usize)
    };
    let topic_name_vec: Vec<u8> = topic_name_slice.to_vec();
    let topic_name = String::from_utf8_lossy(&topic_name_vec);

    let mut consumer_guard = CONSUMER.lock().unwrap();
    if let Some(ref mut consumer) = *consumer_guard {
        match consumer.consume(&topic_name, Duration::from_millis(100)) {
            Ok(Some(payload)) => {
                let boxed = payload.into_boxed_slice();
                let len = boxed.len() as u64;
                let ptr = Box::into_raw(boxed) as *mut u8;

                unsafe {
                    *out_ptr = ptr;
                    *out_len = len;
                    *err_msg_len = 0;
                }
                return 200;
            }
            Ok(None) => {
                let err = b"No message received".to_vec().into_boxed_slice();
                let len = err.len() as u64;
                let ptr = Box::into_raw(err) as *mut u8;

                unsafe {
                    *err_msg_ptr = ptr;
                    *err_msg_len = len;
                }
                return 500;
            }
            Err(err_msg) => {
                let err_bytes = err_msg.into_bytes();
                let boxed = err_bytes.into_boxed_slice();
                let len = boxed.len() as u64;
                let ptr = Box::into_raw(boxed) as *mut u8;

                unsafe {
                    *err_msg_ptr = ptr;
                    *err_msg_len = len;
                }
                return 500;
            }
        }
    }

    500 // Internal Server Error
}

#[unsafe(no_mangle)]
pub extern "C" fn free_buffer(ptr: *mut u8, len: u64) {
    if !ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(slice::from_raw_parts_mut(ptr, len as usize));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;
    use std::ffi::CString;

    fn cstr(s: &str) -> (CString, *const u8, u64) {
        let c_str = CString::new(s).unwrap();
        let ptr = c_str.as_ptr() as *const u8;
        let len = s.len();
        (c_str, ptr, len as u64)
    }

    #[test]
    fn test_set_config_and_produce_success() {
        let (_c_bootstrap, bootstrap_ptr, bootstrap_len) = cstr("localhost:9092");

        let status =  set_config(bootstrap_ptr, bootstrap_len);
        assert_eq!(status, 200, "set_config failed");

        let (_c_topic, topic_ptr, topic_len) = cstr("topic0");
        let (_c_payload, payload_ptr, payload_len) = cstr("test-message");

        let mut err_msg_ptr: *mut u8 = ptr::null_mut();
        let mut err_msg_len: u64 = 0;

        let status = produce(
                topic_ptr,
                topic_len,
                payload_ptr,
                payload_len,
                &mut err_msg_ptr,
                &mut err_msg_len,
            );

        assert_eq!(status, 200, "produce failed");
        assert_eq!(err_msg_len, 0, "error message should be empty on success");
        assert!(err_msg_ptr.is_null(), "error pointer should be null on success");
    }
    #[test]
    fn test_consume_success() {
        let (_c_bootstrap, bootstrap_ptr, bootstrap_len) = cstr("localhost:9092");
        let status = set_config(bootstrap_ptr, bootstrap_len);
        assert_eq!(status, 200);

        let (_c_topic, topic_ptr, topic_len) = cstr("topic0");

        let mut out_ptr: *mut u8 = ptr::null_mut();
        let mut out_len: u64 = 0;
        let mut err_msg_ptr: *mut u8 = ptr::null_mut();
        let mut err_msg_len: u64 = 0;

        let status =
            consume(
                topic_ptr,
                topic_len,
                &mut out_ptr,
                &mut out_len,
                &mut err_msg_ptr,
                &mut err_msg_len,
            );

        if status == 200 {
            assert!(!out_ptr.is_null());
            assert!(out_len > 0);
            let data = unsafe { std::slice::from_raw_parts(out_ptr, out_len as usize) };
            let msg = String::from_utf8_lossy(data);
            println!("consume success: {}", msg);
            free_buffer(out_ptr, out_len);
        } else {
            assert!(!err_msg_ptr.is_null() || err_msg_len > 0);
            let err_data = unsafe { std::slice::from_raw_parts(err_msg_ptr, err_msg_len as usize) };
            let err_msg = String::from_utf8_lossy(err_data);
            println!("consume error: {}", err_msg);
            free_buffer(err_msg_ptr, err_msg_len);
        }
    }
}
