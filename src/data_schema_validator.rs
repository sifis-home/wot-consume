use serde::Deserialize;
use wot_td::thing::DataSchema;

pub(crate) fn find_valid_out<T>(schema: &[DataSchema<(), (), ()>]) -> Option<usize>
where
    T: for<'de> Deserialize<'de>,
{
}
