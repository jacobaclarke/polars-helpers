use polars::lazy::dsl::Expr;
use polars::prelude::*;

pub trait CastDateTime {
    fn cast_datetime_to_date(self) -> Self;
}

impl CastDateTime for Expr {
    fn cast_datetime_to_date(self) -> Self {
        self.str()
            .split(lit("T"))
            .list()
            .first()
            .cast(DataType::Date)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cast_datetime() {
        let df = df! {
          "date" => ["2024-02-03T00:00:00.000"]
        }
        .unwrap()
        .lazy()
        .with_column(col("date").cast_datetime_to_date())
        .collect()
        .unwrap();
        dbg!(df);
    }
}
