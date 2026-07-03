use glob::glob;
use std::fs;

fn main() {
    let root = std::env::var("STONKS_DATA_ROOT").expect("set STONKS_DATA_ROOT to the minute/ dir");
    glob(&format!("{root}/[0-9]*/[0-9]*")).unwrap().flatten().for_each(|p| {
        fs::rename(
            &p,
            p.with_file_name(format!("month={}", p.file_name().unwrap().to_str().unwrap())),
        )
        .unwrap()
    });
    glob(&format!("{root}/[0-9]*")).unwrap().flatten().for_each(|p| {
        fs::rename(
            &p,
            p.with_file_name(format!("year={}", p.file_name().unwrap().to_str().unwrap())),
        )
        .unwrap()
    });
}
