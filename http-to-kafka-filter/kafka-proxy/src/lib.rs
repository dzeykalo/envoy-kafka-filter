use std::slice;

#[unsafe(no_mangle)]
pub extern "C" fn set_config(bootstraps: *const u8, bootstraps_len: u64) -> u64 {
    if bootstraps.is_null() {
        return 500;
    }

    let host_slice = unsafe {
        slice::from_raw_parts(bootstraps, bootstraps_len as usize)
    };
    let bootstraps = String::from_utf8_lossy(host_slice);
    print!("kafka hosts {}", bootstraps);
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
    let payload: Vec<u8> = payload_slice.to_vec();
    print!("payload {:?}", String::from_utf8_lossy(&payload));

    let err_msg_output_len = payload.len() as u64;
    let err_msg_output = payload.into_boxed_slice();
    let err_msg_output_ptr = Box::into_raw(err_msg_output) as *mut u8;

    unsafe {
        *err_msg_ptr = err_msg_output_ptr;
        *err_msg_len = err_msg_output_len;
    }
    200 // OK
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
    let topic_name: Vec<u8> = topic_name_slice.to_vec();
    print!("topic name {}", String::from_utf8_lossy(&topic_name));

    let err_msg_output_len = topic_name.len() as u64;
    let err_msg_output = topic_name.into_boxed_slice();
    let err_msg_output_ptr = Box::into_raw(err_msg_output) as *mut u8;

    unsafe {
        *out_ptr = err_msg_output_ptr;
        *out_len = err_msg_output_len;
        *err_msg_ptr = err_msg_output_ptr;
        *err_msg_len = err_msg_output_len;
    }
    200
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

    #[test]
    fn test_on_data() {
        let topic_name = b"foo";
        let input_data = b"hello world";
        let mut out_ptr: *mut u8 = ptr::null_mut();
        let mut out_len: u64 = 0;

        let status = produce(
            topic_name.as_ptr(), topic_name.len() as u64,
            input_data.as_ptr(), input_data.len() as u64,
            &mut out_ptr, &mut out_len);

        assert_eq!(status, 200);
        assert!(!out_ptr.is_null());
        assert_eq!(out_len, input_data.len() as u64);

        let result_slice = unsafe { slice::from_raw_parts(out_ptr, out_len as usize) };
        assert_eq!(result_slice, input_data.as_ref());

        free_buffer(out_ptr, out_len);
    }
}
