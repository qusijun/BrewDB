//! File storage engine integration.

mod engine;
mod util;

pub use engine::{FileTableEngine, FileTableEngineFactory, FileTableLocationKind};
