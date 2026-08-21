//! Planner wire-codec conversions.

use crate::planner::distributed::{PlanFragment, PlanFragmentId, PlanFragmentKind};
use crate::planner::errors::PlannerError;
use brewdb_prost::planner::v1 as prost;

pub fn encode_plan_fragment(fragment: &PlanFragment) -> Result<prost::PlanFragment, PlannerError> {
    if fragment.root.is_some() || fragment.local_plan.is_some() {
        return Err(PlannerError::InvalidPlan {
            reason: "plan fragment logical plan codec is not implemented yet".to_owned(),
        });
    }
    Ok(prost::PlanFragment {
        fragment_id: Some(prost::PlanFragmentId {
            value: fragment.fragment_id.0,
        }),
        kind: encode_plan_fragment_kind(&fragment.kind) as i32,
        root_logical_plan: Vec::new(),
        local_logical_plan: Vec::new(),
    })
}

pub fn decode_plan_fragment(fragment: prost::PlanFragment) -> Result<PlanFragment, PlannerError> {
    if !fragment.root_logical_plan.is_empty() || !fragment.local_logical_plan.is_empty() {
        return Err(PlannerError::InvalidPlan {
            reason: "plan fragment logical plan codec is not implemented yet".to_owned(),
        });
    }
    let fragment_id = fragment
        .fragment_id
        .ok_or_else(|| PlannerError::InvalidPlan {
            reason: "plan fragment id is missing".to_owned(),
        })?;
    Ok(PlanFragment {
        fragment_id: PlanFragmentId(fragment_id.value),
        kind: decode_plan_fragment_kind(fragment.kind)?,
        root: None,
        local_plan: None,
    })
}

fn encode_plan_fragment_kind(kind: &PlanFragmentKind) -> prost::PlanFragmentKind {
    match kind {
        PlanFragmentKind::Source => prost::PlanFragmentKind::Source,
        PlanFragmentKind::Intermediate => prost::PlanFragmentKind::Intermediate,
        PlanFragmentKind::Root => prost::PlanFragmentKind::Root,
    }
}

fn decode_plan_fragment_kind(kind: i32) -> Result<PlanFragmentKind, PlannerError> {
    match prost::PlanFragmentKind::try_from(kind) {
        Ok(prost::PlanFragmentKind::Source) => Ok(PlanFragmentKind::Source),
        Ok(prost::PlanFragmentKind::Intermediate) => Ok(PlanFragmentKind::Intermediate),
        Ok(prost::PlanFragmentKind::Root) => Ok(PlanFragmentKind::Root),
        Ok(prost::PlanFragmentKind::Unspecified) | Err(_) => Err(PlannerError::InvalidPlan {
            reason: format!("unsupported plan fragment kind: {kind}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_fragment_codec_round_trips_fragment_metadata() {
        let fragment = PlanFragment {
            fragment_id: PlanFragmentId(7),
            kind: PlanFragmentKind::Source,
            root: None,
            local_plan: None,
        };

        let encoded = encode_plan_fragment(&fragment).unwrap();
        assert_eq!(encoded.fragment_id.unwrap().value, 7);
        assert_eq!(encoded.kind, prost::PlanFragmentKind::Source as i32);

        let decoded = decode_plan_fragment(encode_plan_fragment(&fragment).unwrap()).unwrap();
        assert_eq!(decoded, fragment);
    }

    #[test]
    fn plan_fragment_codec_rejects_unknown_fragment_kind() {
        let err = decode_plan_fragment(prost::PlanFragment {
            fragment_id: Some(prost::PlanFragmentId { value: 1 }),
            kind: 99,
            root_logical_plan: Vec::new(),
            local_logical_plan: Vec::new(),
        })
        .unwrap_err();

        assert!(err.to_string().contains("unsupported plan fragment kind"));
    }
}
