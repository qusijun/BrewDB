// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt::{Display, Formatter};
use std::io;
use std::str::FromStr;
use std::time::{Duration, Instant};

use crate::client::{ClientError, QueryResult, render_query_result};

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum MaxRows {
    /// Show all rows in the output.
    Unlimited,
    /// Only show n rows.
    Limited(usize),
}

impl FromStr for MaxRows {
    type Err = String;

    fn from_str(maxrows: &str) -> Result<Self, Self::Err> {
        if maxrows.to_lowercase() == "inf"
            || maxrows.to_lowercase() == "infinite"
            || maxrows.to_lowercase() == "none"
        {
            Ok(Self::Unlimited)
        } else {
            match maxrows.parse::<usize>() {
                Ok(nrows) => Ok(Self::Limited(nrows)),
                _ => Err(format!(
                    "Invalid maxrows {maxrows}. Valid inputs are natural numbers or 'none', 'inf', or 'infinite' for no limit."
                )),
            }
        }
    }
}

impl Display for MaxRows {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unlimited => write!(f, "unlimited"),
            Self::Limited(max_rows) => write!(f, "at most {max_rows}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrintOptions {
    pub quiet: bool,
    pub maxrows: MaxRows,
}

impl Default for PrintOptions {
    fn default() -> Self {
        Self {
            quiet: false,
            maxrows: MaxRows::Unlimited,
        }
    }
}

impl PrintOptions {
    pub fn print_query_result<W: io::Write>(
        &self,
        writer: &mut W,
        result: &QueryResult,
        query_start_time: Instant,
    ) -> Result<(), ClientError> {
        render_query_result(&mut *writer, result)?;

        let formatted_exec_details =
            get_execution_details_formatted(result.rows.len(), self.maxrows, query_start_time);
        self.write_output(writer, &formatted_exec_details)
    }

    fn write_output<W: io::Write>(
        &self,
        writer: &mut W,
        formatted_exec_details: &str,
    ) -> Result<(), ClientError> {
        if !self.quiet {
            writeln!(writer, "{formatted_exec_details}")?;
        }

        Ok(())
    }
}

fn get_execution_details_formatted(
    row_count: usize,
    maxrows: MaxRows,
    query_start_time: Instant,
) -> String {
    get_execution_details_formatted_for_elapsed(row_count, maxrows, query_start_time.elapsed())
}

fn get_execution_details_formatted_for_elapsed(
    row_count: usize,
    maxrows: MaxRows,
    elapsed: Duration,
) -> String {
    let nrows_shown_msg = match maxrows {
        MaxRows::Limited(nrows) if nrows < row_count => {
            format!("(First {nrows} displayed. Use --maxrows to adjust)")
        }
        _ => String::new(),
    };

    format!(
        "{} row(s) fetched. {}\nElapsed {:.3} seconds.\n",
        row_count,
        nrows_shown_msg,
        elapsed.as_secs_f64()
    )
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[test]
    fn max_rows_parses_datafusion_cli_values() {
        assert_eq!("none".parse::<MaxRows>().unwrap(), MaxRows::Unlimited);
        assert_eq!("inf".parse::<MaxRows>().unwrap(), MaxRows::Unlimited);
        assert_eq!("infinite".parse::<MaxRows>().unwrap(), MaxRows::Unlimited);
        assert_eq!("10".parse::<MaxRows>().unwrap(), MaxRows::Limited(10));
        assert!("wat".parse::<MaxRows>().is_err());
    }

    #[test]
    fn print_query_result_prints_datafusion_cli_execution_details() {
        let result = QueryResult {
            headers: vec!["id".to_owned()],
            rows: vec![vec![Some("1".to_owned())], vec![Some("2".to_owned())]],
            command_tag: "SELECT 2".to_owned(),
        };
        let options = PrintOptions::default();
        let started_at = Instant::now() - Duration::from_millis(1234);
        let mut out = Vec::new();

        options
            .print_query_result(&mut out, &result, started_at)
            .unwrap();

        let output = String::from_utf8(out).unwrap();
        assert!(output.contains("SELECT 2\n"));
        assert!(output.contains("2 row(s) fetched. "));
        assert!(output.contains("Elapsed 1."));
    }

    #[test]
    fn execution_details_format_elapsed_to_three_decimal_seconds() {
        let output = get_execution_details_formatted_for_elapsed(
            2,
            MaxRows::Unlimited,
            Duration::from_millis(1234),
        );

        assert_eq!(output, "2 row(s) fetched. \nElapsed 1.234 seconds.\n");
    }

    #[test]
    fn print_query_result_honors_quiet() {
        let result = QueryResult {
            headers: vec![],
            rows: vec![],
            command_tag: "CREATE TABLE".to_owned(),
        };
        let options = PrintOptions {
            quiet: true,
            maxrows: MaxRows::Unlimited,
        };
        let mut out = Vec::new();

        options
            .print_query_result(&mut out, &result, Instant::now())
            .unwrap();

        assert_eq!(String::from_utf8(out).unwrap(), "CREATE TABLE\n");
    }

    #[test]
    fn print_query_result_reports_limited_max_rows() {
        let result = QueryResult {
            headers: vec![],
            rows: vec![vec![], vec![]],
            command_tag: String::new(),
        };
        let options = PrintOptions {
            quiet: false,
            maxrows: MaxRows::Limited(1),
        };
        let mut out = Vec::new();

        options
            .print_query_result(&mut out, &result, Instant::now())
            .unwrap();

        let output = String::from_utf8(out).unwrap();
        assert!(output.contains("(First 1 displayed. Use --maxrows to adjust)"));
    }
}
