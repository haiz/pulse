// Payload transform operations applied before delivery.
// Transforms operate on clones of payloads — the original WAL event is never modified.

/// A dot-delimited field path.
pub type FieldPath = Vec<String>;

/// A transform operation.
#[derive(Debug, Clone)]
pub enum TransformOp {
    /// Set a field to a value (add or overwrite).
    Set { path: FieldPath, value: rmpv::Value },
    /// Remove a field.
    Remove { path: FieldPath },
    /// Rename a field.
    Rename { from: FieldPath, to: FieldPath },
    /// Copy a field's value to another location.
    Copy { from: FieldPath, to: FieldPath },
    /// Set a field only if it doesn't already exist.
    Default { path: FieldPath, value: rmpv::Value },
}

/// A sequence of transform operations.
#[derive(Debug, Clone)]
pub struct TransformPipeline {
    pub ops: Vec<TransformOp>,
}

impl TransformPipeline {
    pub fn new(ops: Vec<TransformOp>) -> Self {
        Self { ops }
    }

    /// Apply all transforms to a mutable payload.
    pub fn apply(&self, payload: &mut rmpv::Value) {
        for op in &self.ops {
            match op {
                TransformOp::Set { path, value } => {
                    set_field(payload, path, value.clone());
                }
                TransformOp::Remove { path } => {
                    remove_field(payload, path);
                }
                TransformOp::Rename { from, to } => {
                    if let Some(val) = remove_field(payload, from) {
                        set_field(payload, to, val);
                    }
                }
                TransformOp::Copy { from, to } => {
                    if let Some(val) = get_field(payload, from) {
                        set_field(payload, to, val.clone());
                    }
                }
                TransformOp::Default { path, value } => {
                    if get_field(payload, path).is_none() {
                        set_field(payload, path, value.clone());
                    }
                }
            }
        }
    }
}

/// Get a field value by path.
fn get_field<'a>(value: &'a rmpv::Value, path: &[String]) -> Option<&'a rmpv::Value> {
    let mut current = value;
    for segment in path {
        match current {
            rmpv::Value::Map(entries) => {
                let found = entries.iter().find(|(k, _)| match k {
                    rmpv::Value::String(s) => s.as_str() == Some(segment.as_str()),
                    _ => false,
                });
                match found {
                    Some((_, v)) => current = v,
                    None => return None,
                }
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Set a field value by path, creating intermediate maps as needed.
fn set_field(value: &mut rmpv::Value, path: &[String], new_val: rmpv::Value) {
    if path.is_empty() {
        return;
    }

    if path.len() == 1 {
        if let rmpv::Value::Map(entries) = value {
            let key = rmpv::Value::String(path[0].clone().into());
            if let Some(entry) = entries.iter_mut().find(|(k, _)| *k == key) {
                entry.1 = new_val;
            } else {
                entries.push((key, new_val));
            }
        }
        return;
    }

    // Navigate to parent, create intermediate maps
    if let rmpv::Value::Map(entries) = value {
        let key = rmpv::Value::String(path[0].clone().into());
        let child = entries.iter_mut().find(|(k, _)| *k == key);

        if let Some(entry) = child {
            set_field(&mut entry.1, &path[1..], new_val);
        } else {
            let mut new_map = rmpv::Value::Map(vec![]);
            set_field(&mut new_map, &path[1..], new_val);
            entries.push((key, new_map));
        }
    }
}

/// Remove a field by path, returning the removed value.
fn remove_field(value: &mut rmpv::Value, path: &[String]) -> Option<rmpv::Value> {
    if path.is_empty() {
        return None;
    }

    if let rmpv::Value::Map(entries) = value {
        if path.len() == 1 {
            let key_str = &path[0];
            let pos = entries.iter().position(|(k, _)| match k {
                rmpv::Value::String(s) => s.as_str() == Some(key_str.as_str()),
                _ => false,
            });
            pos.map(|i| entries.remove(i).1)
        } else {
            let key_str = &path[0];
            let child = entries.iter_mut().find(|(k, _)| match k {
                rmpv::Value::String(s) => s.as_str() == Some(key_str.as_str()),
                _ => false,
            });
            child.and_then(|(_, v)| remove_field(v, &path[1..]))
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_map(data: &[(&str, rmpv::Value)]) -> rmpv::Value {
        rmpv::Value::Map(
            data.iter()
                .map(|(k, v)| (rmpv::Value::String((*k).into()), v.clone()))
                .collect(),
        )
    }

    fn path(s: &str) -> FieldPath {
        s.split('.').map(|p| p.to_string()).collect()
    }

    #[test]
    fn set_field_simple() {
        let mut payload = make_map(&[("a", rmpv::Value::Integer(1.into()))]);
        let pipeline = TransformPipeline::new(vec![TransformOp::Set {
            path: path("b"),
            value: rmpv::Value::String("hello".into()),
        }]);
        pipeline.apply(&mut payload);

        assert!(get_field(&payload, &path("b")).is_some());
    }

    #[test]
    fn set_field_overwrite() {
        let mut payload = make_map(&[("a", rmpv::Value::Integer(1.into()))]);
        let pipeline = TransformPipeline::new(vec![TransformOp::Set {
            path: path("a"),
            value: rmpv::Value::Integer(2.into()),
        }]);
        pipeline.apply(&mut payload);

        let val = get_field(&payload, &path("a")).unwrap();
        assert_eq!(val, &rmpv::Value::Integer(2.into()));
    }

    #[test]
    fn remove_field_op() {
        let mut payload = make_map(&[
            ("keep", rmpv::Value::Integer(1.into())),
            ("remove_me", rmpv::Value::String("secret".into())),
        ]);
        let pipeline = TransformPipeline::new(vec![TransformOp::Remove {
            path: path("remove_me"),
        }]);
        pipeline.apply(&mut payload);

        assert!(get_field(&payload, &path("remove_me")).is_none());
        assert!(get_field(&payload, &path("keep")).is_some());
    }

    #[test]
    fn rename_field_op() {
        let mut payload = make_map(&[("old_name", rmpv::Value::String("value".into()))]);
        let pipeline = TransformPipeline::new(vec![TransformOp::Rename {
            from: path("old_name"),
            to: path("new_name"),
        }]);
        pipeline.apply(&mut payload);

        assert!(get_field(&payload, &path("old_name")).is_none());
        assert!(get_field(&payload, &path("new_name")).is_some());
    }

    #[test]
    fn copy_field_op() {
        let mut payload = make_map(&[("src", rmpv::Value::Integer(42.into()))]);
        let pipeline = TransformPipeline::new(vec![TransformOp::Copy {
            from: path("src"),
            to: path("dst"),
        }]);
        pipeline.apply(&mut payload);

        // Both should exist
        assert_eq!(
            get_field(&payload, &path("src")).unwrap(),
            &rmpv::Value::Integer(42.into())
        );
        assert_eq!(
            get_field(&payload, &path("dst")).unwrap(),
            &rmpv::Value::Integer(42.into())
        );
    }

    #[test]
    fn default_sets_when_missing() {
        let mut payload = make_map(&[("existing", rmpv::Value::Integer(1.into()))]);
        let pipeline = TransformPipeline::new(vec![TransformOp::Default {
            path: path("priority"),
            value: rmpv::Value::String("normal".into()),
        }]);
        pipeline.apply(&mut payload);

        assert_eq!(
            get_field(&payload, &path("priority")).unwrap(),
            &rmpv::Value::String("normal".into())
        );
    }

    #[test]
    fn default_does_not_overwrite() {
        let mut payload = make_map(&[("priority", rmpv::Value::String("high".into()))]);
        let pipeline = TransformPipeline::new(vec![TransformOp::Default {
            path: path("priority"),
            value: rmpv::Value::String("normal".into()),
        }]);
        pipeline.apply(&mut payload);

        assert_eq!(
            get_field(&payload, &path("priority")).unwrap(),
            &rmpv::Value::String("high".into())
        );
    }
}
