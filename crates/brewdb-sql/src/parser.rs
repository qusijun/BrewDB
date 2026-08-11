//! BrewDB SQL parser boundary.

use brewdb_sql_parser::ast::Statement;
use brewdb_sql_parser::dialect::PostgreSqlDialect;
use brewdb_sql_parser::parser::Parser;

use crate::errors::SqlError;
#[derive(Clone, Debug, Default)]
pub struct SqlParser;

impl SqlParser {
    pub fn sql_to_statement(&self, sql: &str) -> Result<Statement, SqlError> {
        let dialect = PostgreSqlDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql)?;
        if statements.is_empty() {
            return Err(SqlError::Parse {
                reason: "parser returned no statement".to_string(),
            });
        }
        if statements.len() > 1 {
            return Err(SqlError::UnsupportedStatement {
                reason: "multi-statement SQL is not supported yet".to_string(),
            });
        }
        Ok(statements.remove(0))
    }
}
