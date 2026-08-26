use crate::parser::ast::Ident;
use datafusion_expr::{col, Expr as DataFusionExpr};

pub(super) fn bind_identifier(ident: &Ident) -> DataFusionExpr {
    col(ident.to_string())
}

pub(super) fn bind_compound_identifier(idents: &[Ident]) -> DataFusionExpr {
    col(idents
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("."))
}
