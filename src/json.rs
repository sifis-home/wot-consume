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

    let a = Number::from(a);
    let b = Number::from(b);

    match (a, b) {
        (Number::PosInt(a), Number::PosInt(b)) => a == b,
        (Number::NegInt(a), Number::NegInt(b)) => a == b,
        (Number::Float(a), Number::Float(b)) => a == b,
        (Number::PosInt(a), Number::Float(b)) | (Number::Float(b), Number::PosInt(a)) => {
            let a_f64 = a as f64;
            a_f64 == b && a_f64 as u64 == a
        }
        (Number::NegInt(a), Number::Float(b)) | (Number::Float(b), Number::NegInt(a)) => {
            let a_f64 = a as f64;
            a_f64 == b && a_f64 as i64 == a
        }
        (Number::PosInt(_), Number::NegInt(_)) | (Number::NegInt(_), Number::PosInt(_)) => false,
    }
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

#[cfg(test)]
mod tests {
    use std::ops::Not;

    use super::*;

    #[test]
    fn eq_numbers_positive() {
        assert!(eq_numbers(&15u32.into(), &15i32.into()));
        assert!(eq_numbers(&15u32.into(), &16u32.into()).not());
    }

    #[test]
    fn eq_numbers_negative() {
        assert!(eq_numbers(&(-15i32).into(), &(-15i64).into()));
        assert!(eq_numbers(&(-15).into(), &(-16).into()).not());
    }

    #[test]
    fn eq_numbers_float() {
        use serde_json::Number;

        assert!(eq_numbers(
            &Number::from_f64(10.2).unwrap(),
            &Number::from_f64(10.2).unwrap(),
        ));
        assert!(eq_numbers(
            &Number::from_f64(10.2).unwrap(),
            &Number::from_f64(10.3).unwrap(),
        )
        .not());
    }

    #[test]
    fn eq_numbers_mixed_pos_neg() {
        assert!(eq_numbers(&15u32.into(), &(-15i32).into()).not());
        assert!(eq_numbers(&(-15i32).into(), &15i32.into()).not());
    }

    #[test]
    fn eq_numbers_mixed_pos_float() {
        use serde_json::Number;

        assert!(eq_numbers(&15u32.into(), &Number::from_f64(15.0).unwrap()));
        assert!(eq_numbers(
            &u32::MAX.into(),
            &Number::from_f64(u32::MAX.into()).unwrap()
        ));

        const REPRESENTABLE_IN_F64: u64 = 2u64.pow(f64::MANTISSA_DIGITS) + 2;
        const NON_REPRESENTABLE_IN_F64: u64 = REPRESENTABLE_IN_F64 + 1;
        // static assertion
        let _: [u8; 1] =
            [0; ((REPRESENTABLE_IN_F64 as f64 as u64) == REPRESENTABLE_IN_F64) as usize];
        let _: [u8; 0] =
            [0; ((NON_REPRESENTABLE_IN_F64 as f64 as u64) == NON_REPRESENTABLE_IN_F64) as usize];

        assert!(eq_numbers(
            &Number::from_f64(REPRESENTABLE_IN_F64 as f64).unwrap(),
            &Number::from_f64(REPRESENTABLE_IN_F64 as f64).unwrap(),
        ));
        assert!(eq_numbers(
            &Number::from_f64(NON_REPRESENTABLE_IN_F64 as f64).unwrap(),
            &Number::from_f64(NON_REPRESENTABLE_IN_F64 as f64).unwrap(),
        ));
        assert!(eq_numbers(
            &REPRESENTABLE_IN_F64.into(),
            &Number::from_f64(REPRESENTABLE_IN_F64 as f64).unwrap(),
        ));
        assert!(eq_numbers(
            &Number::from_f64(REPRESENTABLE_IN_F64 as f64).unwrap(),
            &REPRESENTABLE_IN_F64.into(),
        ));
    }

    #[test]
    fn eq_numbers_mixed_neg_float() {
        use serde_json::Number;

        assert!(eq_numbers(&(-15).into(), &Number::from_f64(-15.0).unwrap()));
        assert!(eq_numbers(
            &i32::MIN.into(),
            &Number::from_f64(i32::MIN.into()).unwrap()
        ));

        const REPRESENTABLE_IN_F64: i64 = -(2i64.pow(f64::MANTISSA_DIGITS)) - 2;
        const NON_REPRESENTABLE_IN_F64: i64 = REPRESENTABLE_IN_F64 - 1;
        // static assertion
        let _: [u8; 1] =
            [0; ((REPRESENTABLE_IN_F64 as f64 as i64) == REPRESENTABLE_IN_F64) as usize];
        let _: [u8; 0] =
            [0; ((NON_REPRESENTABLE_IN_F64 as f64 as i64) == NON_REPRESENTABLE_IN_F64) as usize];

        assert!(eq_numbers(
            &Number::from_f64(REPRESENTABLE_IN_F64 as f64).unwrap(),
            &Number::from_f64(REPRESENTABLE_IN_F64 as f64).unwrap(),
        ));
        assert!(eq_numbers(
            &Number::from_f64(NON_REPRESENTABLE_IN_F64 as f64).unwrap(),
            &Number::from_f64(NON_REPRESENTABLE_IN_F64 as f64).unwrap(),
        ));
        assert!(eq_numbers(
            &REPRESENTABLE_IN_F64.into(),
            &Number::from_f64(REPRESENTABLE_IN_F64 as f64).unwrap(),
        ));
        assert!(eq_numbers(
            &Number::from_f64(REPRESENTABLE_IN_F64 as f64).unwrap(),
            &REPRESENTABLE_IN_F64.into(),
        ));
    }
}
