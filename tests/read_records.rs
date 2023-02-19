mod helpers;
use helpers::*;

#[helpers::test]
async fn read_records_ok() {
    let mut rdr = get_reader("tests/data/cities_ok.csv").await.expect("Data file found");
    let mut max_population = 0;
    let mut max_record = None;
    let mut records = rdr.records();
    while let Some(record) = records.next().await {
        let record = record.expect("Record read correctly");
        if let Some(population) = record.get(3) {
            let population = population.parse::<usize>()
                .expect("Column 4 parsed as integer");
            if population > max_population {
                max_population = population;
                max_record = Some(record.clone());
            }
        }
    }
    assert_eq!(max_record.expect("Max found").get(0), Some("Boston"));
}

#[helpers::test]
async fn read_records_incomplete_row() {
    let mut rdr = get_reader("tests/data/cities_incomplete_row.csv").await.expect("Data file found");
    let mut read_correctly = 0;
    let mut read_errors = Vec::new();
    let mut records = rdr.records();
    while let Some(record) = records.next().await {
        match record {
            Ok(_) => read_correctly += 1,
            Err(e) => read_errors.push(e)
        }
    }
    assert_eq!(read_correctly, 6);
    assert_eq!(read_errors.len(), 1);
    
    // For file with unix newlines.
    let (line, byte) = if cfg!(windows) {
        (2, 67)
    } else {
        (3, 66) // correct value
    };
    assert_eq!(
        read_errors[0].to_string(), 
        format!("CSV error: record 2 (line: {line}, byte: {byte}): found record with 2 fields, but the previous record has 4 fields")
    );
    assert_eq!(
        custom_error_message(&read_errors[0]), 
        format!("Unequal lengths: position = Some(Position {{ byte: {byte}, line: {line}, record: 2 }}), expected_len = 4, len = 2")
    );
}

#[helpers::test]
async fn read_records_non_utf8() {
    let mut rdr = get_reader("tests/data/cities_pl_win1250.csv").await.expect("Data file found");
    let mut read_correctly = 0;
    let mut read_errors = Vec::new();
    let mut records = rdr.records();
    while let Some(record) = records.next().await {
        match record {
            Ok(_) => read_correctly += 1,
            Err(e) => read_errors.push(e)
        }
    }
    assert_eq!(read_correctly, 2);
    assert_eq!(read_errors.len(), 5);
    assert_eq!(
        read_errors[0].to_string().as_str(), 
        "CSV parse error: record 0 (line 1, field: 1, byte: 0): invalid utf-8: invalid UTF-8 in field 1 near byte index 3"
    );
    assert_eq!(
        custom_error_message(&read_errors[0]).as_str(), 
        "Invalid UTF8: position = Some(Position { byte: 0, line: 1, record: 0 }), err = invalid utf-8: invalid UTF-8 in field 1 near byte index 3"
    );
}