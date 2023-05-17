use std::{iter, ops::Not, slice};

use serde::Deserialize;
use wot_td::thing::{self, DataSchema, DataSchemaSubtype};

const MIN_STRING_LEN: u32 = 10;

pub(crate) type FindValidOutIter<'a> =
    iter::Filter<slice::Iter<'a, DataSchema<(), (), ()>>, fn(&&DataSchema<(), (), ()>) -> bool>;

pub(crate) fn find_valid_out<T>(schemas: &[DataSchema<(), (), ()>]) -> FindValidOutIter<'_>
where
    T: for<'de> Deserialize<'de>,
{
    schemas.iter().filter(is_valid_data_schema::<T>)
}

fn is_valid_data_schema<T>(schema: &&DataSchema<(), (), ()>) -> bool
where
    T: for<'de> Deserialize<'de>,
{
    if let Some(constant) = &schema.constant {
        if serde_json::from_value::<T>(constant.to_owned()).is_err() {
            return false;
        }
    }

    if let Some(enumeration) = &schema.enumeration {
        if enumeration
            .iter()
            .any(|variant| serde_json::from_value::<T>(variant.clone()).is_err())
        {
            return false;
        }
    }

    if let Some(one_of) = &schema.one_of {
        if one_of
            .iter()
            .any(|schema| is_valid_data_schema::<T>(&schema))
            .not()
        {
            return false;
        }
    }

    schema
        .subtype
        .as_ref()
        .map_or(true, |subtype| match subtype {
            DataSchemaSubtype::Array(array) => {
                todo!()
            }
            DataSchemaSubtype::Boolean => {
                serde_json::from_value::<T>(true.into()).is_ok()
                    && serde_json::from_value::<T>(false.into()).is_ok()
            }
            DataSchemaSubtype::Number(number) => match (number.minimum, number.maximum) {
                (None, None) => serde_json::from_value::<T>(std::f64::consts::PI.into()).is_ok(),
                (Some(minimum), Some(maximum)) if minimum <= maximum => {
                    let value = match minimum {
                        thing::Minimum::Inclusive(value) => value,
                        thing::Minimum::Exclusive(value) => {
                            let maximum = match maximum {
                                thing::Maximum::Inclusive(x) => x,
                                thing::Maximum::Exclusive(x) => x,
                            };

                            value / 2. + maximum / 2.
                        }
                    };
                    serde_json::from_value::<T>(value.into()).is_ok()
                }
                (Some(_), Some(_)) => false,
                (Some(minimum), None) => {
                    let minimum = match minimum {
                        thing::Minimum::Inclusive(x) => x,
                        thing::Minimum::Exclusive(x) => x,
                    };
                    serde_json::from_value::<T>((minimum + 1.).into()).is_ok()
                }
                (None, Some(maximum)) => {
                    let maximum = match maximum {
                        thing::Maximum::Inclusive(x) => x,
                        thing::Maximum::Exclusive(x) => x,
                    };
                    serde_json::from_value::<T>((maximum - 1.).into()).is_ok()
                }
            },
            DataSchemaSubtype::Integer(integer) => match (integer.minimum, integer.maximum) {
                (None, None) => serde_json::from_value::<T>(42.into()).is_ok(),
                (Some(minimum), Some(maximum)) if minimum <= maximum => {
                    let value = match minimum {
                        thing::Minimum::Inclusive(value) => value,
                        thing::Minimum::Exclusive(value) => value + 1,
                    };
                    serde_json::from_value::<T>(value.into()).is_ok()
                }
                (Some(_), Some(_)) => false,
                (Some(minimum), None) => {
                    let minimum = match minimum {
                        thing::Minimum::Inclusive(x) => x,
                        thing::Minimum::Exclusive(x) => x + 1,
                    };
                    serde_json::from_value::<T>(minimum.into()).is_ok()
                }
                (None, Some(maximum)) => {
                    let maximum = match maximum {
                        thing::Maximum::Inclusive(x) => x,
                        thing::Maximum::Exclusive(x) => x - 1,
                    };
                    serde_json::from_value::<T>(maximum.into()).is_ok()
                }
            },
            DataSchemaSubtype::Object(_) => todo!(),
            DataSchemaSubtype::String(s) => {
                let len = match (s.min_length, s.max_length) {
                    (Some(min), Some(max)) if min <= max => max,
                    (Some(_), Some(_)) => 0,
                    (None, Some(max)) => max,
                    (Some(min), None) if min == 0 => MIN_STRING_LEN,
                    (Some(min), None) => min,
                    (None, None) => MIN_STRING_LEN,
                };

                todo!()
            }
            DataSchemaSubtype::Null => serde_json::from_value::<T>(serde_json::Value::Null).is_ok(),
        })
}
