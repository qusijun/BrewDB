//! Binder context carrying session and catalog lookup handles.

use brewdb_catalog::CatalogService;

use crate::ingress::{SqlRequestContext, SqlSessionContext};

pub struct StatementBindingContext<'a> {
    pub session: &'a SqlSessionContext,
    pub request: &'a SqlRequestContext,
    pub catalog_service: &'a CatalogService,
}
