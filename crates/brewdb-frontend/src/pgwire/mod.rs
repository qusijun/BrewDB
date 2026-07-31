//! PostgreSQL wire protocol adapter shells.

use crate::errors::FrontendError;
use crate::result::{FrontendResponse, Notice, ResultField};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgWireRequest {
    Startup {
        user: String,
        database: Option<String>,
    },
    Query {
        sql: String,
    },
    Terminate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgWireResponse {
    AuthenticationOk,
    ReadyForQuery,
    CommandComplete {
        tag: String,
    },
    RowDescription {
        fields: Vec<ResultField>,
    },
    NoticeResponse {
        severity: &'static str,
        message: String,
    },
}

#[derive(Clone, Debug, Default)]
pub struct PgWireCodec;

impl PgWireCodec {
    pub fn decode_query(&self, payload: &[u8]) -> Result<PgWireRequest, FrontendError> {
        let sql = std::str::from_utf8(payload).map_err(|_| {
            FrontendError::UnsupportedProtocolMessage {
                message: "query payload is not valid UTF-8".to_string(),
            }
        })?;

        if sql.trim().is_empty() {
            return Err(FrontendError::InvalidRequest {
                reason: "pgwire query payload was empty".to_string(),
            });
        }

        Ok(PgWireRequest::Query {
            sql: sql.trim().to_string(),
        })
    }

    pub fn encode_response(&self, response: &FrontendResponse) -> Vec<PgWireResponse> {
        let mut frames = Vec::new();
        if !response.result.fields.is_empty() {
            frames.push(PgWireResponse::RowDescription {
                fields: response.result.fields.clone(),
            });
        }
        frames.push(PgWireResponse::CommandComplete {
            tag: response.result.command_tag.as_str().to_string(),
        });
        frames.extend(response.notices.iter().map(encode_notice));
        frames.push(PgWireResponse::ReadyForQuery);
        frames
    }
}

fn encode_notice(notice: &Notice) -> PgWireResponse {
    PgWireResponse::NoticeResponse {
        severity: notice.severity,
        message: notice.message.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::{PgWireCodec, PgWireRequest, PgWireResponse};
    use crate::result::{FrontendResponse, Notice, QueryResultOutput, ResultField};

    #[test]
    fn codec_decodes_simple_query_payload() {
        let codec = PgWireCodec;
        let request = codec.decode_query(b"select 1\n").unwrap();

        assert_eq!(
            request,
            PgWireRequest::Query {
                sql: "select 1".to_string()
            }
        );
    }

    #[test]
    fn codec_encodes_query_result_frames() {
        let codec = PgWireCodec;
        let response = FrontendResponse::new(QueryResultOutput::query(
            "SELECT 1",
            1,
            vec![ResultField::new("?column?", "INT8")],
        ))
        .with_notice(Notice::info("ok"));
        let frames = codec.encode_response(&response);

        assert_eq!(
            frames,
            vec![
                PgWireResponse::RowDescription {
                    fields: vec![ResultField::new("?column?", "INT8")]
                },
                PgWireResponse::CommandComplete {
                    tag: "SELECT 1".to_string()
                },
                PgWireResponse::NoticeResponse {
                    severity: "INFO",
                    message: "ok".to_string()
                },
                PgWireResponse::ReadyForQuery,
            ]
        );
    }
}
