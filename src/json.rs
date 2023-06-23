use std::array;

#[derive(Debug, Clone, Copy)]
pub struct ValueRef<'a>(pub &'a serde_json::Value);

impl<'a> ValueRef<'a> {
    pub fn to_refs(self) -> ValueRefs<'a> {
        ValueRefs(array::from_ref(self.0).as_slice())
    }
}

impl<'a> From<&'a serde_json::Value> for ValueRef<'a> {
    fn from(value: &'a serde_json::Value) -> Self {
        Self(value)
    }
}

impl PartialEq for ValueRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self.0, other.0) {
            (serde_json::Value::Null, serde_json::Value::Null) => true,
            (serde_json::Value::Bool(a), serde_json::Value::Bool(b)) => a == b,
            (serde_json::Value::Number(a), serde_json::Value::Number(b)) => eq_numbers(a, b),
            (serde_json::Value::String(a), serde_json::Value::String(b)) => a == b,
            (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
                a.iter().map(ValueRef).eq(b.iter().map(ValueRef))
            }
            (serde_json::Value::Object(a), serde_json::Value::Object(b)) => a
                .iter()
                .map(|(k, v)| (k, ValueRef(v)))
                .eq(b.iter().map(|(k, v)| (k, ValueRef(v)))),

            _ => false,
        }
    }
}

impl Eq for ValueRef<'_> {}

impl PartialEq<serde_json::Value> for ValueRef<'_> {
    #[inline]
    fn eq(&self, other: &serde_json::Value) -> bool {
        self == &ValueRef(other)
    }
}

impl PartialEq<&serde_json::Value> for ValueRef<'_> {
    #[inline]
    fn eq(&self, other: &&serde_json::Value) -> bool {
        self == &ValueRef(other)
    }
}

fn eq_numbers(a: &serde_json::Number, b: &serde_json::Number) -> bool {
    enum Number {
        PosInt(u64),
        NegInt(i64),
        Float(f64),
    }

    impl From<&serde_json::Number> for Number {
        fn from(value: &serde_json::Number) -> Self {
            if let Some(value) = value.as_u64() {
                Self::PosInt(value)
            } else if let Some(value) = value.as_i64() {
                Self::NegInt(value)
            } else {
                Self::Float(value.as_f64().unwrap())
            }
        }
    }

    todo!()
}

#[derive(Debug, Clone, Copy)]
pub struct ValueRefs<'a>(pub &'a [serde_json::Value]);

impl<'a> From<&'a [serde_json::Value]> for ValueRefs<'a> {
    fn from(value: &'a [serde_json::Value]) -> Self {
        Self(value)
    }
}

#[cfg(test)]
macro_rules! json_ref {
    ($($tt:tt)*) => {
        crate::json::ValueRef(&serde_json::json!($($tt)*))
    };
}
#[cfg(test)]
pub(super) use json_ref;
