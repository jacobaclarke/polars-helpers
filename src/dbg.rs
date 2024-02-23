use polars::prelude::*;

/// A trait to debug a dataframe
pub trait Debug {
    /// Print out the dataframe
    /// # Example
    /// ```
    /// #use polars::prelude::*;
    /// #use polars_helpers::Debug;
    /// let df = df!("a" => [1, 2, 3]);
    /// df.debug();
    /// ```
    fn debug(self) -> Self;

    /// Print out the dataframe with a label
    /// # Example
    /// ```
    /// #use polars::prelude::*;
    /// #use polars_helpers::Debug;
    /// let df = df!("a" => [1, 2, 3]);
    /// df.debug_labeled("my dataframe");
    /// ```
    fn debug_labeled(self, label: &str) -> Self;

    fn debug_closure(self, f: impl FnOnce(&DataFrame)) -> Self;
}
impl Debug for DataFrame {
    fn debug(self) -> Self {
        #[cfg(debug_assertions)]
        dbg!(&self);
        self
    }

    fn debug_labeled(self, label: &str) -> Self {
        #[cfg(debug_assertions)]
        dbg!(label, &self);
        self
    }

    fn debug_closure(self, f: impl FnOnce(&DataFrame)) -> Self {
        #[cfg(debug_assertions)]
        {
            f(&self);
        }
        self
    }
}

impl Debug for LazyFrame {
    fn debug(self) -> Self {
        self.collect().unwrap().debug().lazy()
    }

    fn debug_labeled(self, label: &str) -> Self {
        self.collect().unwrap().debug_labeled(label).lazy()
    }

    fn debug_closure(self, f: impl FnOnce(&DataFrame)) -> Self {
        self.collect().unwrap().debug_closure(f).lazy()
    }
}
