use polars::prelude::*;
use reqwest::{self, get};
use std::collections::hash_map::DefaultHasher;
use std::fs::{create_dir, File};
use std::hash::{Hash, Hasher};
use std::io::Write;


/// A builder to read a csv from a url.
pub struct UrlReader<'a> {
  /// Cache the file in the cache dir
  cache: bool,
  /// The url to read from
  url: &'a str,
}

impl<'a> UrlReader<'a> {
  pub fn new(url: &'a str) -> Self {
      UrlReader {
          cache: false,
          url,
      }
  }

  /// Cache the file in the cache dir
  pub fn cache(&mut self) -> &mut Self {
      self.cache = true;
      self
  }

  /// Read the csv from the url and save it to disk
  /// Returns a CsvReader
  pub async fn finish(&self) -> PolarsResult<CsvReader<'a, File>> {
      // create cache dir
      let mut h = DefaultHasher::new();
      self.url.hash(&mut h);

      let cache_subpath = h.finish().to_string();
      let mut cache_path = dirs::cache_dir().expect("No cache dir available");
      create_dir(&cache_path).ok();

      cache_path.push("datars");

      //exists ok
      create_dir(&cache_path).ok();
      cache_path.push(cache_subpath);

      if self.cache && cache_path.is_file() {
          println!("Reading from cache: {:?}", cache_path);
          return CsvReader::from_path(cache_path);
      }

      println!("{:?}", cache_path);
      let mut file = File::create(&cache_path)?;
      let url = self.url.to_string();
      let bytes = get(url)
          .await
          .expect("Unable to retreive url")
          .bytes()
          .await
          .expect("Unable to obtain bytes from request");
      file.write_all(&bytes)?;

      CsvReader::from_path(cache_path)
  }
}

pub enum Cache {
  On,
  Off
}

pub trait Url {
  fn from_url(url: &str, cache: &Cache) -> Self;
}

impl Url for DataFrame {
  #[tokio::main]
  async fn from_url(url: &str, cache: &Cache) -> Self {
      let mut reader = UrlReader::new(url);
      if let Cache::On = cache {
          reader.cache();
      }
      reader
          .finish()
          .await
          .expect("Unable to fetch url")
          .finish()
          .expect("Unable to read fetched file to dataframe")
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_hash() {
      let mut h = std::collections::hash_map::DefaultHasher::new();
      "https://google.com".hash(&mut h);
      println!("{:?}", h.finish());
  }
}