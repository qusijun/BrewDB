//! Stage boundary kinds and output contracts.

use brewdb_core::execution::BoundaryKind;

/// Execution-time release semantics applied to a stage boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BoundarySemantics {
    Pipelined,
    Materialized,
    Barriered,
}

impl BoundarySemantics {
    pub fn is_streaming_friendly(self) -> bool {
        matches!(self, Self::Pipelined)
    }
}

/// Declarative release condition for downstream work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReleaseCondition {
    AnyUpstreamPartitionReady,
    AllUpstreamPartitionsReady,
    BoundaryArtifactsPublished,
}

/// Execution boundary descriptor shared by slicing and scheduling layers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BoundaryDescriptor {
    pub kind: BoundaryKind,
    pub semantics: BoundarySemantics,
    pub release_condition: ReleaseCondition,
}

impl BoundaryDescriptor {
    pub fn for_kind(kind: BoundaryKind) -> Self {
        match kind {
            BoundaryKind::Exchange => Self {
                kind,
                semantics: BoundarySemantics::Pipelined,
                release_condition: ReleaseCondition::AnyUpstreamPartitionReady,
            },
            BoundaryKind::Materialization => Self {
                kind,
                semantics: BoundarySemantics::Materialized,
                release_condition: ReleaseCondition::BoundaryArtifactsPublished,
            },
            BoundaryKind::Selection => Self {
                kind,
                semantics: BoundarySemantics::Barriered,
                release_condition: ReleaseCondition::AllUpstreamPartitionsReady,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use brewdb_core::execution::BoundaryKind;

    use super::{BoundaryDescriptor, BoundarySemantics, ReleaseCondition};

    #[test]
    fn boundary_defaults_match_scheduler_direction() {
        let exchange = BoundaryDescriptor::for_kind(BoundaryKind::Exchange);
        let materialize = BoundaryDescriptor::for_kind(BoundaryKind::Materialization);
        let selection = BoundaryDescriptor::for_kind(BoundaryKind::Selection);

        assert_eq!(exchange.semantics, BoundarySemantics::Pipelined);
        assert_eq!(
            materialize.release_condition,
            ReleaseCondition::BoundaryArtifactsPublished
        );
        assert_eq!(selection.semantics, BoundarySemantics::Barriered);
        assert!(exchange.semantics.is_streaming_friendly());
    }
}
