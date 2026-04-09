//! C ABI for Pulse SDK.
//!
//! Provides opaque handle-based API for use from C, Python (ctypes/cffi),
//! Go (CGo), and other languages.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

use pulse_sdk::{Pulse, PulseBuilder};

// ─── Error Handling ───

/// Error codes returned by FFI functions.
#[repr(C)]
pub enum PulseErrorCode {
    Ok = 0,
    ConnectionFailed = 1,
    PublishFailed = 2,
    SubscribeFailed = 3,
    Timeout = 4,
    InvalidArg = 5,
    InternalError = 6,
}

// Thread-local last error message.
thread_local! {
    static LAST_ERROR: std::cell::RefCell<Option<CString>> = const { std::cell::RefCell::new(None) };
}

fn set_last_error(msg: &str) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = CString::new(msg).ok();
    });
}

/// Get the last error message. Returns NULL if no error.
/// The returned pointer is valid until the next FFI call on this thread.
#[no_mangle]
pub extern "C" fn pulse_last_error() -> *const c_char {
    LAST_ERROR.with(|e| {
        e.borrow()
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(ptr::null())
    })
}

// ─── Opaque Handles ───

/// Opaque handle to a Pulse client.
pub struct PulseHandle {
    client: Pulse,
    runtime: tokio::runtime::Runtime,
}

// ─── Connection ───

/// Connect to a Pulse broker. Returns an opaque handle.
///
/// # Safety
/// `addr`, `service_id`, and `namespace` must be valid UTF-8 C strings.
/// The returned handle must be freed with `pulse_disconnect`.
#[no_mangle]
pub unsafe extern "C" fn pulse_connect(
    addr: *const c_char,
    service_id: *const c_char,
    namespace: *const c_char,
    api_key: *const c_char,
) -> *mut PulseHandle {
    let addr = match safe_cstr(addr) {
        Some(s) => s,
        None => {
            set_last_error("invalid addr");
            return ptr::null_mut();
        }
    };
    let service_id = match safe_cstr(service_id) {
        Some(s) => s,
        None => {
            set_last_error("invalid service_id");
            return ptr::null_mut();
        }
    };
    let namespace = match safe_cstr(namespace) {
        Some(s) => s,
        None => {
            set_last_error("invalid namespace");
            return ptr::null_mut();
        }
    };
    let api_key = safe_cstr(api_key).unwrap_or("");

    let socket_addr = match addr.parse() {
        Ok(a) => a,
        Err(e) => {
            set_last_error(&format!("invalid address: {e}"));
            return ptr::null_mut();
        }
    };

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            set_last_error(&format!("runtime error: {e}"));
            return ptr::null_mut();
        }
    };

    let client = runtime.block_on(async {
        PulseBuilder::new(service_id, namespace)
            .addr(socket_addr)
            .api_key(api_key)
            .auto_reconnect(false)
            .connect()
            .await
    });

    match client {
        Ok(client) => Box::into_raw(Box::new(PulseHandle { client, runtime })),
        Err(e) => {
            set_last_error(&format!("connect failed: {e}"));
            ptr::null_mut()
        }
    }
}

/// Disconnect and free a Pulse handle.
///
/// # Safety
/// `handle` must be a valid pointer returned by `pulse_connect`, or NULL.
#[no_mangle]
pub unsafe extern "C" fn pulse_disconnect(handle: *mut PulseHandle) {
    if !handle.is_null() {
        drop(Box::from_raw(handle));
    }
}

// ─── Publish ───

/// Publish an event. `data` is a UTF-8 JSON string.
///
/// # Safety
/// `handle` must be a valid Pulse handle. `topic` and `data` must be valid C strings.
#[no_mangle]
pub unsafe extern "C" fn pulse_publish(
    handle: *mut PulseHandle,
    topic: *const c_char,
    data: *const c_char,
) -> PulseErrorCode {
    let handle = match handle.as_mut() {
        Some(h) => h,
        None => {
            set_last_error("null handle");
            return PulseErrorCode::InvalidArg;
        }
    };
    let topic = match safe_cstr(topic) {
        Some(s) => s,
        None => {
            set_last_error("invalid topic");
            return PulseErrorCode::InvalidArg;
        }
    };
    let data_str = safe_cstr(data).unwrap_or("null");
    let rmpv_data = rmpv::Value::String(data_str.into());

    let result = handle
        .runtime
        .block_on(handle.client.publish(topic, rmpv_data, None));

    match result {
        Ok(_) => PulseErrorCode::Ok,
        Err(e) => {
            set_last_error(&e.to_string());
            PulseErrorCode::PublishFailed
        }
    }
}

// ─── Subscribe ───

/// Subscribe to a topic pattern.
///
/// # Safety
/// `handle` must be a valid Pulse handle. `topic` must be a valid C string.
#[no_mangle]
pub unsafe extern "C" fn pulse_subscribe(
    handle: *mut PulseHandle,
    topic: *const c_char,
) -> PulseErrorCode {
    let handle = match handle.as_mut() {
        Some(h) => h,
        None => {
            set_last_error("null handle");
            return PulseErrorCode::InvalidArg;
        }
    };
    let topic = match safe_cstr(topic) {
        Some(s) => s,
        None => {
            set_last_error("invalid topic");
            return PulseErrorCode::InvalidArg;
        }
    };

    let result = handle
        .runtime
        .block_on(handle.client.subscribe(topic, None));

    match result {
        Ok(_) => PulseErrorCode::Ok,
        Err(e) => {
            set_last_error(&e.to_string());
            PulseErrorCode::SubscribeFailed
        }
    }
}

// ─── Helpers ───

unsafe fn safe_cstr<'a>(ptr: *const c_char) -> Option<&'a str> {
    if ptr.is_null() {
        return None;
    }
    CStr::from_ptr(ptr).to_str().ok()
}
