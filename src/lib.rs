use polars::prelude::*;
use std::str::FromStr;
mod map;
pub use map::*;
mod url;
pub use url::*;
mod dates;
pub use dates::*;
mod dbg;
pub use dbg::*;
mod string;
// / A trait to validate a dataframe
pub trait Validate {
    /// Validate the shape of the dataframe
    fn has_shape(self, shape: (usize, usize)) -> Self;
    /// Validate the columns of the dataframe
    fn has_cols(self, columns: Vec<&str>) -> Self;
    /// Validate the length of the dataframe
    fn has_length(self, length: Comparison) -> Self;
    /// Validate the dataframe with an expression
    fn validate<S>(self, expr: Expr) -> Self;
}

pub enum Comparison {
    GreaterThan(usize),
    LessThan(usize),
    Equal(usize),
}

impl Validate for LazyFrame {
    fn has_shape(self, shape: (usize, usize)) -> Self {
        self.collect().unwrap().has_shape(shape).lazy()
    }
    fn has_cols(self, columns: Vec<&str>) -> Self {
        self.collect().unwrap().has_cols(columns).lazy()
    }
    fn has_length(self, length: Comparison) -> Self {
        self.collect().unwrap().has_length(length).lazy()
    }
    fn validate<S>(self, expr: Expr) -> Self {
        self.collect().unwrap().validate::<S>(expr).lazy()
    }
}

impl Validate for DataFrame {
    fn has_shape(self, shape: (usize, usize)) -> Self {
        assert_eq!(self.shape(), shape, "Checking Shape");
        self
    }
    fn has_cols(self, columns: Vec<&str>) -> Self {
        assert_eq!(self.get_column_names(), columns, "Checking Columns");
        self
    }
    fn has_length(self, length: Comparison) -> Self {
        match length {
            Comparison::GreaterThan(len) => {
                assert!(
                    self.height() > len,
                    "Checking length is greater than {}",
                    len
                );
            }
            Comparison::LessThan(len) => {
                assert!(self.height() < len, "Checking length is less than {}", len);
            }
            Comparison::Equal(len) => {
                assert_eq!(self.height(), len, "Checking length is equal to {}", len);
            }
        }
        self
    }
    fn validate<S>(self, expr: Expr) -> Self {
        assert!(
            self.clone().lazy().select([expr]).collect().unwrap()[0]
                .bool()
                .expect("Validation did not return a boolean")
                .get(0)
                .unwrap(),
            "Validation failed"
        );
        self
    }
}

pub trait DataFrameTesting {
    fn get_cell<T>(&self, col: &str, row: usize) -> T
    where
        T: FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Debug;
}

impl DataFrameTesting for DataFrame {
    fn get_cell<T>(&self, col: &str, row: usize) -> T
    where
        T: FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Debug,
    {
        let temp = self
            .column(col)
            .expect("Col does not exist")
            .cast(&DataType::String)
            .expect("Unable to cast value to string");

        temp.get(row)
            .expect("Row does not exist")
            .get_str()
            .expect("Cell is empty")
            .parse::<T>()
            .expect("Type did not match when parsing cell")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_get_cell() {
        let df = df!("one" => [1]).unwrap();
        assert_eq!(df.get_cell::<usize>("one", 0), 1);
    }
}
