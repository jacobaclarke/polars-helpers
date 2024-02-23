use itertools::Itertools;
use polars::frame::row::Row;
use polars::prelude::*;

pub trait MapDataFrame: DataFrameOps {
    fn from_map<
        V: polars::prelude::NumericNative,
        U: AsRef<[V]>,
        K: AsRef<str>,
        B: AsRef<[(K, U)]>,
    >(
        vals: B,
    ) -> DataFrame {
        let rows = vals
            .as_ref()
            .iter()
            .flat_map(|(key, values)| {
                values.as_ref().iter().map(move |value| {
                    Row::new(vec![AnyValue::String(key.as_ref()), AnyValue::from(*value)])
                })
            })
            .collect_vec();

        let mut df = DataFrame::from_rows(&rows).unwrap();

        df.set_column_names(&["key", "value"]).unwrap();
        df
    }

    fn from_string_map<'a, U: AsRef<[&'a str]>, B: AsRef<[(&'a str, U)]>>(vals: B) -> DataFrame {
        let rows = vals
            .as_ref()
            .iter()
            .flat_map(|(key, values)| {
                values
                    .as_ref()
                    .iter()
                    .map(|value| Row::new(vec![AnyValue::String(key), AnyValue::String(value)]))
            })
            .collect_vec();

        let mut df = DataFrame::from_rows(&rows).unwrap();

        df.set_column_names(&["key", "value"]).unwrap();
        df
    }
}

impl MapDataFrame for DataFrame {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_iter() {
        let df = DataFrame::from_string_map([("50+ yr", vec!["50-64 years", "65+ years"])]);
        assert_eq!(df.get_column_names(), &["key", "value"]);
        assert_eq!(df.shape(), (2, 2));
    }

    #[test]
    fn from_iter_numeric() {
        let vales = vec![("hi", vec![1_u32, 2_u32, 3_u32])];
        DataFrame::from_map(vales);
    }
}
