use std::fs;

use glob::glob;

fn main() {
    let root = std::env::var("BACKTEST_DATA_DIR").expect("set BACKTEST_DATA_DIR");
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
