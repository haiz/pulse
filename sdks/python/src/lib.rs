use std::collections::HashMap;
use std::net::SocketAddr;

use pyo3::exceptions::{PyConnectionError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use pulse_sdk::{PulseBuilder, PulseError};

// ─── JSON ↔ rmpv conversion ───

fn py_to_rmpv(_py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<rmpv::Value> {
    if obj.is_none() {
        return Ok(rmpv::Value::Nil);
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(rmpv::Value::Boolean(b));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(rmpv::Value::Integer(i.into()));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(rmpv::Value::F64(f));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(rmpv::Value::String(s.into()));
    }
    if let Ok(list) = obj.downcast::<PyList>() {
        let items: PyResult<Vec<rmpv::Value>> =
            list.iter().map(|item| py_to_rmpv(_py, &item)).collect();
        return Ok(rmpv::Value::Array(items?));
    }
    if let Ok(dict) = obj.downcast::<PyDict>() {
        let entries: PyResult<Vec<(rmpv::Value, rmpv::Value)>> = dict
            .iter()
            .map(|(k, v)| {
                let key = rmpv::Value::String(k.extract::<String>()?.into());
                let val = py_to_rmpv(_py, &v)?;
                Ok((key, val))
            })
            .collect();
        return Ok(rmpv::Value::Map(entries?));
    }
    Err(PyValueError::new_err("unsupported Python type"))
}

fn rmpv_to_py(py: Python<'_>, val: &rmpv::Value) -> PyResult<PyObject> {
    match val {
        rmpv::Value::Nil => Ok(py.None()),
        rmpv::Value::Boolean(b) => Ok(b.to_object(py)),
        rmpv::Value::Integer(i) => {
            if let Some(n) = i.as_i64() {
                Ok(n.to_object(py))
            } else if let Some(n) = i.as_u64() {
                Ok(n.to_object(py))
            } else {
                Ok(py.None())
            }
        }
        rmpv::Value::F32(f) => Ok((*f as f64).to_object(py)),
        rmpv::Value::F64(f) => Ok(f.to_object(py)),
        rmpv::Value::String(s) => {
            let s = s.as_str().unwrap_or("");
            Ok(s.to_object(py))
        }
        rmpv::Value::Binary(b) => Ok(b.to_object(py)),
        rmpv::Value::Array(arr) => {
            let items: PyResult<Vec<PyObject>> =
                arr.iter().map(|item| rmpv_to_py(py, item)).collect();
            Ok(items?.to_object(py))
        }
        rmpv::Value::Map(entries) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in entries {
                let key = match k {
                    rmpv::Value::String(s) => s.as_str().unwrap_or("").to_string(),
                    _ => format!("{k}"),
                };
                dict.set_item(key, rmpv_to_py(py, v)?)?;
            }
            Ok(dict.to_object(py))
        }
        _ => Ok(py.None()),
    }
}

fn pulse_err_to_py(e: PulseError) -> PyErr {
    match e {
        PulseError::Connection(_) => PyConnectionError::new_err(e.to_string()),
        _ => PyRuntimeError::new_err(e.to_string()),
    }
}

// ─── Event wrapper ───

/// An event received from the broker.
#[pyclass]
#[derive(Clone)]
struct Event {
    #[pyo3(get)]
    msg_id: String,
    #[pyo3(get)]
    topic: String,
    #[pyo3(get)]
    attempt: u32,
    data_rmpv: rmpv::Value,
    #[pyo3(get)]
    headers: HashMap<String, String>,
}

#[pymethods]
impl Event {
    #[getter]
    fn data(&self, py: Python<'_>) -> PyResult<PyObject> {
        rmpv_to_py(py, &self.data_rmpv)
    }

    fn __repr__(&self) -> String {
        format!("Event(topic='{}', msg_id='{}')", self.topic, self.msg_id)
    }
}

// ─── Sync Client ───

/// Synchronous Pulse client.
///
/// Usage:
///     client = Pulse.connect("127.0.0.1:4222", "my-service", "default")
///     client.publish("order.created", {"id": 42})
#[pyclass]
struct Pulse {
    client: std::sync::Mutex<pulse_sdk::Pulse>,
    runtime: tokio::runtime::Runtime,
}

#[pymethods]
impl Pulse {
    /// Connect to a Pulse broker.
    #[staticmethod]
    #[pyo3(signature = (addr, service_id, namespace, api_key=""))]
    fn connect(addr: &str, service_id: &str, namespace: &str, api_key: &str) -> PyResult<Self> {
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| PyValueError::new_err(format!("invalid address '{addr}': {e}")))?;

        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("runtime error: {e}")))?;

        let client = runtime.block_on(async {
            PulseBuilder::new(service_id, namespace)
                .addr(socket_addr)
                .api_key(api_key)
                .connect()
                .await
        });

        let client = client.map_err(pulse_err_to_py)?;
        Ok(Self {
            client: std::sync::Mutex::new(client),
            runtime,
        })
    }

    /// Publish an event to a topic.
    #[pyo3(signature = (topic, data, headers=None))]
    fn publish(
        &self,
        py: Python<'_>,
        topic: &str,
        data: &Bound<'_, PyAny>,
        headers: Option<HashMap<String, String>>,
    ) -> PyResult<String> {
        let rmpv_data = py_to_rmpv(py, data)?;
        let opts = headers.map(|h| pulse_sdk::types::PublishOpts {
            headers: h,
            msg_id: None,
        });

        let mut client = self.client.lock().unwrap();
        let msg_id = self
            .runtime
            .block_on(client.publish(topic, rmpv_data, opts))
            .map_err(pulse_err_to_py)?;

        Ok(msg_id.to_string())
    }

    /// Subscribe to a topic pattern.
    #[pyo3(signature = (topic, group=None))]
    fn subscribe(&self, topic: &str, group: Option<String>) -> PyResult<()> {
        let opts = Some(pulse_sdk::types::SubscribeOpts {
            group,
            ..Default::default()
        });

        let mut client = self.client.lock().unwrap();
        self.runtime
            .block_on(client.subscribe(topic, opts))
            .map_err(pulse_err_to_py)?;

        Ok(())
    }

    #[getter]
    fn broker_id(&self) -> String {
        let client = self.client.lock().unwrap();
        client.broker_id().to_string()
    }

    fn __repr__(&self) -> String {
        let client = self.client.lock().unwrap();
        format!("Pulse(broker='{}')", client.broker_id())
    }
}

// ─── Module ───

#[pymodule]
fn pulse_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Pulse>()?;
    m.add_class::<Event>()?;
    Ok(())
}
