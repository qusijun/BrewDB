//! Local sibling-repo binding for the customized Lakekeeper source tree.

use lakekeeper_local::service::{
    PaimonCommitState, PaimonTableInfo, ProjectId as LakekeeperProjectId,
    WarehouseId as LakekeeperWarehouseId,
};

/// Local-source binding handle into the customized Lakekeeper workspace.
///
/// This module intentionally depends on the sibling checkout at `../lakekeeper`
/// through a Cargo path dependency instead of any published crate.
#[derive(Clone, Debug)]
pub struct LakekeeperLocalBinding {
    pub project_id: LakekeeperProjectId,
    pub warehouse_id: LakekeeperWarehouseId,
}

impl LakekeeperLocalBinding {
    pub fn new(project_id: LakekeeperProjectId, warehouse_id: LakekeeperWarehouseId) -> Self {
        Self {
            project_id,
            warehouse_id,
        }
    }

    pub fn table_full_name(&self, table: &PaimonTableInfo) -> String {
        table.tabular_ident.to_string()
    }

    pub const fn commit_state_label(state: PaimonCommitState) -> &'static str {
        match state {
            PaimonCommitState::Stable => "stable",
            PaimonCommitState::PendingPublish => "pending-publish",
            PaimonCommitState::PublishFailed => "publish-failed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LakekeeperLocalBinding;

    #[test]
    fn commit_state_label_matches_local_lakekeeper_enum() {
        assert_eq!(
            LakekeeperLocalBinding::commit_state_label(
                lakekeeper_local::service::PaimonCommitState::Stable
            ),
            "stable"
        );
        assert_eq!(
            LakekeeperLocalBinding::commit_state_label(
                lakekeeper_local::service::PaimonCommitState::PendingPublish
            ),
            "pending-publish"
        );
    }
}
