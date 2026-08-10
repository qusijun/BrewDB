//! BrewDB SQL parser boundary.

use brewdb_sql_parser::ast::Statement as AstStatement;
use brewdb_sql_parser::dialect::PostgreSqlDialect;
use brewdb_sql_parser::parser::Parser;

use crate::errors::SqlError;
use crate::statement::{ParsedStatement, ParsedStatementKind};

#[derive(Clone, Debug, Default)]
pub struct SqlParser;

impl SqlParser {
    pub fn parse_one(&self, sql: &str) -> Result<ParsedStatement, SqlError> {
        let statement_text = sql.trim().trim_end_matches(';').trim().to_string();
        if statement_text.is_empty() {
            return Err(SqlError::InvalidRequest {
                reason: "SQL text must not be empty".to_string(),
            });
        }

        let ast = parse_ast(&statement_text)?;
        Ok(ParsedStatement {
            statement_text,
            kind: ParsedStatementKind::from_ast(&ast),
            ast,
        })
    }
}

fn parse_ast(sql: &str) -> Result<AstStatement, SqlError> {
    let dialect = PostgreSqlDialect {};
    let mut ast = Parser::parse_sql(&dialect, sql)?;
    if ast.is_empty() {
        return Err(SqlError::Parse {
            reason: "parser returned no statement".to_string(),
        });
    }
    if ast.len() > 1 {
        return Err(SqlError::UnsupportedStatement {
            reason: "multi-statement SQL is not supported yet".to_string(),
        });
    }
    Ok(ast.remove(0))
}
