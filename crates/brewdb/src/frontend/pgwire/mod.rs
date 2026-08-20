//! PostgreSQL wire protocol plugin.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use uuid::Uuid;

use crate::common::diagnostics::DiagnosticError;
use crate::frontend::auth::{AuthContext, AuthMethod, StaticAuthenticator};
use crate::frontend::errors::FrontendError;
use crate::frontend::protocol::{
    FrontendProtocolPlugin, FrontendProtocolRequest, FrontendProtocolResponse, SqlRequestHandler,
};
use crate::frontend::result::{FrontendResponse, Notice, ResultField};
use crate::frontend::session::{
    ClientCapabilities, ClientConnectionContext, ClientDefaults, FrontendService,
    OpenClientSession, RequestContext,
};

pub type PgWireRequest = FrontendProtocolRequest;
pub type PgWireResponse = FrontendProtocolResponse;

#[derive(Clone, Debug, Default)]
pub struct PgWireCodec;

impl PgWireCodec {
    pub fn decode_query(&self, payload: &[u8]) -> Result<PgWireRequest, FrontendError> {
        let sql = std::str::from_utf8(payload)
            .map_err(|_| protocol_error("query payload is not valid UTF-8"))?;

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

    pub fn serve_connection_io<S: Read + Write>(
        &self,
        mut stream: S,
        frontend: FrontendService,
        defaults: ClientDefaults,
        handler: Arc<dyn SqlRequestHandler>,
    ) -> Result<(), FrontendError> {
        let (user_name, database_name) = read_startup(&mut stream)?;
        let session = frontend.open_session(
            &StaticAuthenticator,
            OpenClientSession {
                auth: auth_context(user_name, database_name.clone()),
                defaults: client_defaults(defaults, database_name.clone()),
                connection: Some(ClientConnectionContext::new(Uuid::new_v4(), "pgwire")),
                capabilities: ClientCapabilities {
                    supports_prepared_statements: false,
                    supports_portals: false,
                    supports_streaming_results: true,
                },
            },
        )?;

        write_authentication_ok(&mut stream)?;
        write_ready_for_query(&mut stream)?;

        loop {
            let (message_type, payload) = read_message(&mut stream)?;
            match message_type {
                b'Q' => {
                    let sql = decode_cstring(&payload)?;
                    match frontend
                        .build_request(&session, RequestContext::new(Uuid::new_v4()), sql)
                        .and_then(|request| handler.execute(&request))
                    {
                        Ok(result) => {
                            write_query_result(&mut stream, &result.response, &result.batches)?;
                            write_ready_for_query(&mut stream)?;
                        }
                        Err(error) => {
                            write_error_response(&mut stream, &error)?;
                            write_ready_for_query(&mut stream)?;
                        }
                    }
                }
                b'X' => return Ok(()),
                _ => write_error_response(
                    &mut stream,
                    &FrontendError::UnsupportedProtocolMessage {
                        message: "unsupported frontend protocol message".to_owned(),
                    },
                )?,
            }
        }
    }
}

impl FrontendProtocolPlugin for PgWireCodec {
    fn protocol_name(&self) -> &'static str {
        "pgwire"
    }

    fn decode_request(&self, payload: &[u8]) -> Result<FrontendProtocolRequest, FrontendError> {
        self.decode_query(payload)
    }

    fn encode_response(&self, response: &FrontendResponse) -> Vec<FrontendProtocolResponse> {
        PgWireCodec::encode_response(self, response)
    }

    fn serve_connection(
        &self,
        stream: TcpStream,
        frontend: FrontendService,
        defaults: ClientDefaults,
        handler: Arc<dyn SqlRequestHandler>,
    ) -> Result<(), FrontendError> {
        self.serve_connection_io(stream, frontend, defaults, handler)
    }
}

fn auth_context(user_name: String, database_name: Option<String>) -> AuthContext {
    let mut context = AuthContext::new(user_name, AuthMethod::Trust);
    if let Some(database_name) = database_name {
        context = context.with_database(database_name);
    }
    context
}

fn client_defaults(mut defaults: ClientDefaults, database_name: Option<String>) -> ClientDefaults {
    if let Some(database_name) = database_name {
        defaults = defaults.with_database(database_name);
    }
    defaults
}

fn read_startup<S: Read + Write>(
    stream: &mut S,
) -> Result<(String, Option<String>), FrontendError> {
    let payload = loop {
        let payload = read_startup_payload(stream)?;
        if payload.len() < 4 {
            return Err(protocol_error("startup packet was too short"));
        }
        let protocol_version = i32::from_be_bytes(payload[..4].try_into().unwrap());
        match protocol_version {
            80_877_103 | 80_877_104 => {
                stream
                    .write_all(b"N")
                    .map_err(|error| protocol_error(&format!("failed to reject TLS: {error}")))?;
            }
            196_608 => break payload,
            _ => return Err(protocol_error("only PostgreSQL protocol 3.0 is supported")),
        }
    };

    let parameters = parse_startup_parameters(&payload[4..])?;
    let user = parameters
        .get("user")
        .cloned()
        .ok_or_else(|| protocol_error("startup packet did not contain a user"))?;
    Ok((user, parameters.get("database").cloned()))
}

fn read_startup_payload<S: Read>(stream: &mut S) -> Result<Vec<u8>, FrontendError> {
    let length = read_i32(stream)?;
    if !(8..=16 * 1024 * 1024).contains(&length) {
        return Err(protocol_error("invalid startup packet length"));
    }
    let mut payload = vec![0; length as usize - 4];
    stream
        .read_exact(&mut payload)
        .map_err(|error| protocol_error(&format!("failed to read startup packet: {error}")))?;
    Ok(payload)
}

fn parse_startup_parameters(
    payload: &[u8],
) -> Result<std::collections::BTreeMap<String, String>, FrontendError> {
    let mut parameters = std::collections::BTreeMap::new();
    let mut offset = 0;
    while offset < payload.len() {
        let (key, next) = read_cstring_at(payload, offset)?;
        offset = next;
        if key.is_empty() {
            break;
        }
        let (value, next) = read_cstring_at(payload, offset)?;
        offset = next;
        parameters.insert(key, value);
    }
    Ok(parameters)
}

fn read_message<S: Read>(stream: &mut S) -> Result<(u8, Vec<u8>), FrontendError> {
    let mut message_type = [0; 1];
    stream
        .read_exact(&mut message_type)
        .map_err(|error| protocol_error(&format!("failed to read message type: {error}")))?;
    let length = read_i32(stream)?;
    if !(4..=16 * 1024 * 1024).contains(&length) {
        return Err(protocol_error("invalid frontend message length"));
    }
    let mut payload = vec![0; length as usize - 4];
    stream
        .read_exact(&mut payload)
        .map_err(|error| protocol_error(&format!("failed to read frontend message: {error}")))?;
    Ok((message_type[0], payload))
}

fn read_i32<S: Read>(stream: &mut S) -> Result<i32, FrontendError> {
    let mut bytes = [0; 4];
    stream
        .read_exact(&mut bytes)
        .map_err(|error| protocol_error(&format!("failed to read packet length: {error}")))?;
    Ok(i32::from_be_bytes(bytes))
}

fn read_cstring_at(payload: &[u8], offset: usize) -> Result<(String, usize), FrontendError> {
    let end = payload[offset..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|position| offset + position)
        .ok_or_else(|| protocol_error("unterminated PostgreSQL string"))?;
    let value = std::str::from_utf8(&payload[offset..end])
        .map_err(|_| protocol_error("PostgreSQL string was not valid UTF-8"))?
        .to_owned();
    Ok((value, end + 1))
}

fn decode_cstring(payload: &[u8]) -> Result<String, FrontendError> {
    let (value, next) = read_cstring_at(payload, 0)?;
    if next != payload.len() {
        return Err(protocol_error("query packet contains trailing bytes"));
    }
    Ok(value)
}

fn write_authentication_ok<S: Write>(stream: &mut S) -> Result<(), FrontendError> {
    write_frame(stream, b'R', &0_i32.to_be_bytes())
}

fn write_ready_for_query<S: Write>(stream: &mut S) -> Result<(), FrontendError> {
    write_frame(stream, b'Z', b"I")
}

fn write_query_result<S: Write>(
    stream: &mut S,
    response: &FrontendResponse,
    batches: &[RecordBatch],
) -> Result<(), FrontendError> {
    let fields = if !response.result.fields.is_empty() {
        response.result.fields.clone()
    } else {
        batches
            .first()
            .map(|batch| {
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| ResultField::new(field.name(), field.data_type().to_string()))
                    .collect()
            })
            .unwrap_or_default()
    };

    if !fields.is_empty() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(fields.len() as i16).to_be_bytes());
        for field in fields {
            payload.extend_from_slice(field.name.as_bytes());
            payload.push(0);
            payload.extend_from_slice(&0_i32.to_be_bytes());
            payload.extend_from_slice(&0_i16.to_be_bytes());
            payload.extend_from_slice(&oid_for_type(&field.data_type).to_be_bytes());
            payload.extend_from_slice(&(-1_i16).to_be_bytes());
            payload.extend_from_slice(&0_i32.to_be_bytes());
            payload.extend_from_slice(&0_i16.to_be_bytes());
        }
        write_frame(stream, b'T', &payload)?;
    }

    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut payload = Vec::new();
            payload.extend_from_slice(&(batch.num_columns() as i16).to_be_bytes());
            for column in batch.columns() {
                match array_value_as_text(column.as_ref(), row) {
                    Some(value) => {
                        payload.extend_from_slice(&(value.len() as i32).to_be_bytes());
                        payload.extend_from_slice(value.as_bytes());
                    }
                    None => payload.extend_from_slice(&(-1_i32).to_be_bytes()),
                }
            }
            write_frame(stream, b'D', &payload)?;
        }
    }

    let mut command_tag = response.result.command_tag.as_str().as_bytes().to_vec();
    command_tag.push(0);
    write_frame(stream, b'C', &command_tag)
}

fn write_error_response<S: Write>(
    stream: &mut S,
    error: &FrontendError,
) -> Result<(), FrontendError> {
    let mut payload = Vec::new();
    let message = error.to_string();
    let code = error.error_code().as_str();
    payload.extend_from_slice(b"SERROR\0");
    payload.extend_from_slice(b"C");
    payload.extend_from_slice(code.as_bytes());
    payload.push(0);
    payload.extend_from_slice(b"M");
    payload.extend_from_slice(message.as_bytes());
    payload.extend_from_slice(b"\0\0");
    write_frame(stream, b'E', &payload)
}

fn write_frame<S: Write>(
    stream: &mut S,
    message_type: u8,
    payload: &[u8],
) -> Result<(), FrontendError> {
    let length = (payload.len() + 4) as i32;
    stream
        .write_all(&[message_type])
        .and_then(|_| stream.write_all(&length.to_be_bytes()))
        .and_then(|_| stream.write_all(payload))
        .map_err(|error| protocol_error(&format!("failed to write pgwire frame: {error}")))
}

fn oid_for_type(data_type: &str) -> i32 {
    let data_type = data_type.to_ascii_lowercase();
    if data_type.contains("int64") || data_type.contains("int8") {
        20
    } else if data_type.contains("int32") || data_type.contains("int4") {
        23
    } else if data_type.contains("bool") {
        16
    } else if data_type.contains("float64") || data_type.contains("double") {
        701
    } else {
        25
    }
}

fn array_value_as_text(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        return Some(array.value(row).to_string());
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(array.value(row).to_string());
    }
    if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
        return Some(array.value(row).to_string());
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        return Some(array.value(row).to_string());
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Some(array.value(row).to_owned());
    }
    Some(format!("{array:?}"))
}

fn protocol_error(message: &str) -> FrontendError {
    FrontendError::UnsupportedProtocolMessage {
        message: message.to_owned(),
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
    use std::io::{Read, Write};

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{PgWireCodec, PgWireRequest, PgWireResponse};
    use crate::frontend::errors::FrontendError;
    use crate::frontend::protocol::{SqlExecutionResult, SqlRequestHandler};
    use crate::frontend::result::{FrontendResponse, Notice, QueryResultOutput, ResultField};
    use crate::frontend::session::{ClientDefaults, FrontendService, SqlRequest};
    use crate::frontend::MANAGED_PAIMON_CATALOG_NAME;
    use std::sync::Arc;

    struct TestHandler;

    impl SqlRequestHandler for TestHandler {
        fn execute(&self, _request: &SqlRequest) -> Result<SqlExecutionResult, FrontendError> {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "count",
                DataType::Int64,
                false,
            )]));
            let values: ArrayRef = Arc::new(Int64Array::from(vec![3]));
            let batch = RecordBatch::try_new(schema, vec![values]).unwrap();
            Ok(SqlExecutionResult {
                response: FrontendResponse::new(QueryResultOutput::query(
                    "SELECT",
                    1,
                    vec![ResultField::new("count", "Int64")],
                )),
                batches: vec![batch],
            })
        }
    }

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

    #[test]
    fn pgwire_plugin_serves_startup_and_simple_query() {
        let (server_stream, mut client) = std::os::unix::net::UnixStream::pair().unwrap();
        let server = std::thread::spawn(move || {
            PgWireCodec
                .serve_connection_io(
                    server_stream,
                    FrontendService::new(),
                    ClientDefaults::default().with_catalog(MANAGED_PAIMON_CATALOG_NAME),
                    Arc::new(TestHandler),
                )
                .unwrap();
        });

        let startup_payload = [
            &196_608_i32.to_be_bytes()[..],
            b"user\0brew\0database\0prod\0\0",
        ]
        .concat();
        write_startup(&mut client, &startup_payload);

        assert_eq!(read_frame(&mut client).0, b'R');
        assert_eq!(read_frame(&mut client).0, b'Z');

        write_message(&mut client, b'Q', b"select 1\0");
        let response_types = [
            read_frame(&mut client).0,
            read_frame(&mut client).0,
            read_frame(&mut client).0,
            read_frame(&mut client).0,
        ];
        assert_eq!(response_types, [b'T', b'D', b'C', b'Z']);

        write_message(&mut client, b'X', &[]);
        drop(client);
        server.join().unwrap();
    }

    fn write_startup(stream: &mut impl Write, payload: &[u8]) {
        let length = (payload.len() + 4) as i32;
        stream.write_all(&length.to_be_bytes()).unwrap();
        stream.write_all(payload).unwrap();
    }

    fn write_message(stream: &mut impl Write, message_type: u8, payload: &[u8]) {
        let length = (payload.len() + 4) as i32;
        stream.write_all(&[message_type]).unwrap();
        stream.write_all(&length.to_be_bytes()).unwrap();
        stream.write_all(payload).unwrap();
    }

    fn read_frame(stream: &mut impl Read) -> (u8, Vec<u8>) {
        let mut message_type = [0; 1];
        stream.read_exact(&mut message_type).unwrap();
        let mut length = [0; 4];
        stream.read_exact(&mut length).unwrap();
        let mut payload = vec![0; i32::from_be_bytes(length) as usize - 4];
        stream.read_exact(&mut payload).unwrap();
        (message_type[0], payload)
    }
}
