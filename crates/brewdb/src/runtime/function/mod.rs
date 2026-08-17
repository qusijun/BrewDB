//! Runtime-owned DataFusion function extensions.

use datafusion::prelude::SessionContext;

mod paimon;

pub fn register_internal_functions(session: &SessionContext) {
    session.register_udf(paimon::hash_udf());
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn registers_internal_paimon_hash_function() {
        let session = SessionContext::new();
        super::register_internal_functions(&session);

        let batches = session
            .sql("select paimon_hash(cast(1 as int), cast(4 as int))")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let bucket = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(bucket.value(0) >= 0);
        assert!(bucket.value(0) < 4);
    }
}
