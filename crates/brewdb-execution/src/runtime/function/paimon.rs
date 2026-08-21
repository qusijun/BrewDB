use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array};
use arrow::datatypes::DataType;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};

const PAIMON_HASH_FUNCTION_NAME: &str = "paimon_hash";

pub(super) fn hash_udf() -> ScalarUDF {
    create_udf(
        PAIMON_HASH_FUNCTION_NAME,
        vec![DataType::Int32, DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(paimon_hash),
    )
}

fn paimon_hash(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Execution(format!(
            "{PAIMON_HASH_FUNCTION_NAME} expects exactly two arguments"
        )));
    }

    let input_len = args
        .iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            ColumnarValue::Scalar(_) => None,
        })
        .unwrap_or(1);
    let key = args[0].to_array(input_len)?;
    let bucket_count = args[1].to_array(input_len)?;
    let bucket_count = bucket_count
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{PAIMON_HASH_FUNCTION_NAME} bucket count must be Int32"
            ))
        })?;

    let mut hashes = vec![0; input_len];
    create_hashes(&[key], &RandomState::default(), &mut hashes)?;
    let buckets = (0..input_len)
        .map(|row| {
            if bucket_count.is_null(row) {
                None
            } else {
                let count = bucket_count.value(row);
                if count <= 0 {
                    None
                } else {
                    Some((hashes[row] % count as u64) as i32)
                }
            }
        })
        .collect::<Int32Array>();
    let result = Arc::new(buckets) as ArrayRef;
    if args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
    {
        return Ok(ColumnarValue::Scalar(
            datafusion_common::ScalarValue::try_from_array(&result, 0)?,
        ));
    }
    Ok(ColumnarValue::Array(result))
}
