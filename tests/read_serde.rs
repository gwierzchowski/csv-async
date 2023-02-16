#![cfg(feature = "with_serde")]

use serde::Deserialize;

mod helpers;
use helpers::*;

#[allow(dead_code)]
#[derive(Deserialize, Clone)]
struct City {
    city: String,
    region: String,
    country: String,
    population: usize,
}

#[helpers::test]
async fn read_serde_ok() {
    let des = get_deserializer("tests/data/cities_ok.csv").await.expect("Data file found");
    let mut max_population = 0;
    let mut max_record = None;
    let mut records = des.into_deserialize::<City>();
    while let Some(record) = records.next().await {
        let record = record.expect("Record read correctly");
        if record.population > max_population {
            max_population = record.population;
            max_record = Some(record.clone());
        }
    }
    assert_eq!(&max_record.expect("Max found").city, "Boston");
}

#[helpers::test]
async fn read_serde_incomplete_row() {
    let des = get_deserializer("tests/data/cities_incomplete_row.csv").await.expect("Data file found");
    let mut read_correctly = 0;
    let mut read_errors = Vec::new();
    let mut records = des.into_deserialize::<City>();
    while let Some(record) = records.next().await {
        match record {
            Ok(_) => read_correctly += 1,
            Err(e) => read_errors.push(e)
        }
    }
    assert_eq!(read_correctly, 6);
    assert_eq!(read_errors.len(), 1);
    assert_eq!(
        read_errors[0].to_string().as_str(), 
        "CSV error: record 2 (line: 3, byte: 66): found record with 2 fields, but the previous record has 4 fields"
    );
    assert_eq!(
        custom_error_message(&read_errors[0]).as_str(), 
        "Unequal lengths: position = Some(Position { byte: 66, line: 3, record: 2 }), expected_len = 4, len = 2"
    );
}

#[helpers::test]
async fn read_serde_non_utf8() {
    let des = get_deserializer("tests/data/cities_pl_win1250.csv").await.expect("Data file found");
    let mut read_correctly = 0;
    let mut read_errors = Vec::new();
    let mut records = des.into_deserialize::<City>();
    while let Some(record) = records.next().await {
        match record {
            Ok(_) => read_correctly += 1,
            Err(e) => read_errors.push(e)
        }
    }
    assert_eq!(read_correctly, 0);
    assert_eq!(read_errors.len(), 7);
    assert_eq!(
        read_errors[0].to_string().as_str(), 
        "CSV parse error: record 1 (line 2, field: 1, byte: 29): invalid utf-8: invalid UTF-8 in field 1 near byte index 3"
    );
    assert_eq!(
        custom_error_message(&read_errors[0]).as_str(), 
        "Invalid UTF8: position = Some(Position { byte: 29, line: 2, record: 1 }), err = invalid utf-8: invalid UTF-8 in field 1 near byte index 3"
    );
}

#[helpers::test]
async fn read_serde_non_int() {
    let des = get_deserializer("tests/data/cities_non_int.csv").await.expect("Data file found");
    let mut read_correctly = 0;
    let mut read_errors = Vec::new();
    let mut records = des.into_deserialize::<City>();
    while let Some(record) = records.next().await {
        match record {
            Ok(_) => read_correctly += 1,
            Err(e) => read_errors.push(e)
        }
    }
    assert_eq!(read_correctly, 6);
    assert_eq!(read_errors.len(), 1);
    assert_eq!(
        read_errors[0].to_string().as_str(), 
        "CSV deserialize error: record 3 (line 4, byte: 101): field 4: invalid digit found in string"
    );
    assert_eq!(custom_error_message(
        &read_errors[0]).as_str(), 
        "Deserialize error: position = Some(Position { byte: 101, line: 4, record: 3 }), field = Some(3): Error parsing integer: invalid digit found in string"
    );
}
