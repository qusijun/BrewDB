//! SQL parser boundary built on DataFusion's sqlparser AST.

use datafusion_sql::sqlparser::ast::Statement as AstStatement;
use datafusion_sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_sql::sqlparser::parser::Parser;

use crate::errors::SqlError;
use crate::statement::{ParsedStatement, ParsedStatementKind};

#[derive(Clone, Debug, Default)]
pub struct SqlParser;

impl SqlParser {
    pub fn parse_one(&self, sql: &str) -> Result<ParsedStatement, SqlError> {
        let statement_text = sql.trim().to_string();
        if statement_text.is_empty() {
            return Err(SqlError::InvalidRequest {
                reason: "SQL text must not be empty".to_string(),
            });
        }

        let dialect = PostgreSqlDialect {};
        let mut ast = Parser::parse_sql(&dialect, &statement_text)?;
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

        let ast = ast.remove(0);
        Ok(ParsedStatement {
            statement_text,
            kind: ParsedStatementKind::from_ast(&ast),
            ast,
        })
    }
}

#[allow(dead_code)]
fn _assert_ast_type(statement: AstStatement) -> AstStatement {
    statement
}
