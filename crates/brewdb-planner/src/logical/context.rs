use std::sync::Arc;

use crate::catalog::TableCatalogEntry;
use crate::parser::ast::{Ident, ObjectName, TableAlias};
use crate::planner::errors::PlannerError;
use crate::planner::logical::table_source::DefaultTableSource;
use datafusion_common::TableReference;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{AggregateUDF, LogicalPlan as DataFusionLogicalPlan, ScalarUDF, TableSource};
use std::cell::RefCell;
use std::collections::BTreeMap;

pub(super) struct QueryPlannerContext<'a> {
    tables: &'a [TableCatalogEntry],
    function_registry: &'a dyn FunctionRegistry,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
    ctes: RefCell<BTreeMap<String, DataFusionLogicalPlan>>,
}

impl<'a> QueryPlannerContext<'a> {
    pub(super) fn new(
        tables: &'a [TableCatalogEntry],
        function_registry: &'a dyn FunctionRegistry,
    ) -> Self {
        Self {
            tables,
            function_registry,
            expr_planners: vec![
                Arc::new(datafusion_functions::datetime::planner::DatetimeFunctionPlanner),
                Arc::new(datafusion_functions::unicode::planner::UnicodeFunctionPlanner),
                Arc::new(datafusion_functions_aggregate::planner::AggregateFunctionPlanner),
            ],
            ctes: RefCell::new(BTreeMap::new()),
        }
    }

    pub(super) fn tables(&self) -> &'a [TableCatalogEntry] {
        self.tables
    }

    pub(super) fn function_registry(&self) -> &'a dyn FunctionRegistry {
        self.function_registry
    }

    pub(super) fn expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }

    pub(super) fn scalar_function(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        let name = name.to_ascii_lowercase();
        self.function_registry.udf(&name).ok().or_else(|| {
            self.function_registry
                .udfs()
                .into_iter()
                .find_map(|udf_name| {
                    let udf = self.function_registry.udf(&udf_name).ok()?;
                    udf.aliases()
                        .iter()
                        .any(|alias| alias.eq_ignore_ascii_case(&name))
                        .then_some(udf)
                })
        })
    }

    pub(super) fn aggregate_function(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        let name = name.to_ascii_lowercase();
        self.function_registry.udaf(&name).ok().or_else(|| {
            self.function_registry
                .udafs()
                .into_iter()
                .find_map(|udaf_name| {
                    let udaf = self.function_registry.udaf(&udaf_name).ok()?;
                    udaf.aliases()
                        .iter()
                        .any(|alias| alias.eq_ignore_ascii_case(&name))
                        .then_some(udaf)
                })
        })
    }

    pub(super) fn resolve_table(
        &self,
        name: &ObjectName,
    ) -> Result<TableCatalogEntry, PlannerError> {
        let parts = object_name_parts(name)?;
        let matches_path = |table: &TableCatalogEntry| match parts.as_slice() {
            [table_name] => table.path.table() == *table_name,
            [database_name, table_name] => {
                table.path.database() == *database_name && table.path.table() == *table_name
            }
            [catalog_name, database_name, table_name] => {
                table.path.catalog() == *catalog_name
                    && table.path.database() == *database_name
                    && table.path.table() == *table_name
            }
            _ => false,
        };
        let mut matches = self.tables.iter().filter(|table| matches_path(table));
        let Some(table) = matches.next() else {
            return Err(PlannerError::InvalidPlan {
                reason: format!("table `{name}` is not available in query scope"),
            });
        };
        if matches.next().is_some() {
            return Err(PlannerError::InvalidPlan {
                reason: format!("table `{name}` is ambiguous in query scope"),
            });
        }
        Ok(table.clone())
    }

    pub(super) fn table_source(&self, table: TableCatalogEntry) -> Arc<dyn TableSource> {
        Arc::new(DefaultTableSource::new(table))
    }

    pub(super) fn contains_cte(&self, cte_name: &str) -> bool {
        self.ctes.borrow().contains_key(cte_name)
    }

    pub(super) fn insert_cte(&self, cte_name: String, plan: DataFusionLogicalPlan) {
        self.ctes.borrow_mut().insert(cte_name, plan);
    }

    pub(super) fn remove_cte(&self, cte_name: &str) {
        self.ctes.borrow_mut().remove(cte_name);
    }

    pub(super) fn cte(
        &self,
        name: &ObjectName,
    ) -> Result<Option<DataFusionLogicalPlan>, PlannerError> {
        let Some(cte_name) = cte_name_from_object_name(name)? else {
            return Ok(None);
        };
        Ok(self.ctes.borrow().get(&cte_name).cloned())
    }

    pub(super) fn table_reference_for_scan(
        &self,
        name: &ObjectName,
        alias: &Option<TableAlias>,
    ) -> Result<TableReference, PlannerError> {
        if let Some(alias) = alias {
            return Ok(TableReference::bare(identifier_table_reference_part(
                &alias.name,
            )));
        }
        let parts = name
            .0
            .iter()
            .map(|part| {
                part.as_ident()
                    .map(identifier_table_reference_part)
                    .ok_or_else(|| PlannerError::UnsupportedPlan {
                        reason: format!("unsupported table name part `{part}`"),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect::<Vec<_>>();
        match parts.as_slice() {
            [table] => Ok(TableReference::bare(table.clone())),
            [schema, table] => Ok(TableReference::partial(schema.clone(), table.clone())),
            [catalog, schema, table] => Ok(TableReference::full(
                catalog.clone(),
                schema.clone(),
                table.clone(),
            )),
            _ => Err(PlannerError::InvalidPlan {
                reason: format!("invalid table name `{name}`"),
            }),
        }
    }
}

pub(super) fn cte_name_from_ident(ident: &Ident) -> String {
    identifier_table_reference_part(ident)
}

pub(super) fn cte_name_from_object_name(name: &ObjectName) -> Result<Option<String>, PlannerError> {
    match name.0.as_slice() {
        [part] => part
            .as_ident()
            .map(|ident| Some(identifier_table_reference_part(ident)))
            .ok_or_else(|| PlannerError::UnsupportedPlan {
                reason: format!("unsupported table name part `{part}`"),
            }),
        _ => Ok(None),
    }
}

fn object_name_parts(name: &ObjectName) -> Result<Vec<String>, PlannerError> {
    name.0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|ident| ident.value.clone())
                .ok_or_else(|| PlannerError::UnsupportedPlan {
                    reason: format!("unsupported table name part `{part}`"),
                })
        })
        .collect()
}

fn identifier_table_reference_part(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}
