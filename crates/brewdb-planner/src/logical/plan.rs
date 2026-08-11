//! BrewDB logical plan extensions for nodes DataFusion does not model.

use std::any::Any;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, LogicalPlan as DataFusionLogicalPlan, UserDefinedLogicalNode};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub enum LogicalPlanNode {
    Show(Show),
    Ddl(Ddl),
}

impl LogicalPlanNode {
    pub fn show(&self) -> Option<&Show> {
        match self {
            Self::Show(show) => Some(show),
            _ => None,
        }
    }

    pub fn schema_ref() -> &'static DFSchemaRef {
        static SCHEMA: OnceLock<DFSchemaRef> = OnceLock::new();
        SCHEMA.get_or_init(|| Arc::new(DFSchema::empty()))
    }
}

impl UserDefinedLogicalNode for LogicalPlanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        match self {
            Self::Show(_) => "Show",
            Self::Ddl(Ddl::CreateDatabase(_)) => "CreateDatabase",
            Self::Ddl(Ddl::DropDatabase(_)) => "DropDatabase",
        }
    }

    fn inputs(&self) -> Vec<&DataFusionLogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        Self::schema_ref()
    }

    fn check_invariants(
        &self,
        _check: datafusion_expr::logical_plan::InvariantLevel,
    ) -> Result<()> {
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {:?}", self.name(), self)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<DataFusionLogicalPlan>,
    ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        Ok(Arc::new(self.clone()))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut state = state;
        self.hash(&mut state);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    fn dyn_ord(&self, other: &dyn UserDefinedLogicalNode) -> Option<Ordering> {
        other
            .as_any()
            .downcast_ref::<Self>()
            .and_then(|other| self.partial_cmp(other))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub enum Show {
    Catalogs,
    Databases {
        catalog_name: String,
    },
    Tables {
        catalog_name: String,
        database_name: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub enum Ddl {
    CreateDatabase(CreateDatabase),
    DropDatabase(DropDatabase),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct CreateDatabase {
    pub catalog_name: String,
    pub database_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct DropDatabase {
    pub catalog_name: String,
    pub database_name: String,
}
