use std::env;
use std::error::Error;
use std::fmt;
use std::io::{self, IsTerminal, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::time::Instant;

use brewdb_common::defaults::DEFAULT_DATABASE_NAME;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use crate::command::{Command, CommandAction};
use crate::print_options::PrintOptions;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientOptions {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    pub query: Option<String>,
    pub print_options: PrintOptions,
}

impl ClientOptions {
    pub fn from_env() -> Result<Self, ClientError> {
        Self::from_args(env::args().skip(1))
    }

    pub fn from_args<I, S>(args: I) -> Result<Self, ClientError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut host = "127.0.0.1".to_owned();
        let mut port = 5432_u16;
        let mut user = env::var("USER").unwrap_or_else(|_| "brew".to_owned());
        let mut database = DEFAULT_DATABASE_NAME.to_owned();
        let mut query = None;
        let mut print_options = PrintOptions::default();

        let mut iter = args.into_iter().map(Into::into).peekable();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "-h" | "--host" => host = take_value("--host", &mut iter)?,
                "-p" | "--port" => {
                    let value = take_value("--port", &mut iter)?;
                    port = value.parse().map_err(|_| ClientError::InvalidArgs {
                        reason: format!("invalid port `{value}`"),
                    })?;
                }
                "-U" | "--user" => user = take_value("--user", &mut iter)?,
                "-d" | "--database" => database = take_value("--database", &mut iter)?,
                "-c" | "--execute" => query = Some(take_value("--execute", &mut iter)?),
                "-q" | "--quiet" => print_options.quiet = true,
                "--maxrows" => {
                    let value = take_value("--maxrows", &mut iter)?;
                    print_options.maxrows = value
                        .parse()
                        .map_err(|reason| ClientError::InvalidArgs { reason })?;
                }
                "-?" | "--help" => {
                    return Err(ClientError::Usage(usage()));
                }
                other if other.starts_with('-') => {
                    return Err(ClientError::InvalidArgs {
                        reason: format!("unknown flag `{other}`"),
                    });
                }
                other => {
                    let mut sql = other.to_owned();
                    for next in iter {
                        sql.push(' ');
                        sql.push_str(&next);
                    }
                    query = Some(sql);
                    break;
                }
            }
        }

        Ok(Self {
            host,
            port,
            user,
            database,
            query,
            print_options,
        })
    }
}

#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    InvalidArgs {
        reason: String,
    },
    LineEditor {
        reason: String,
    },
    Usage(String),
    Protocol {
        reason: String,
    },
    Server {
        reason: String,
        error_code: Option<String>,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::InvalidArgs { reason } => write!(f, "{reason}"),
            Self::LineEditor { reason } => write!(f, "{reason}"),
            Self::Usage(message) => write!(f, "{message}"),
            Self::Protocol { reason } => write!(f, "{reason}"),
            Self::Server { reason, error_code } => match error_code {
                Some(error_code) => write!(f, "{error_code}: {reason}"),
                None => write!(f, "{reason}"),
            },
        }
    }
}

impl Error for ClientError {}

impl From<io::Error> for ClientError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

pub fn run() -> Result<(), ClientError> {
    let options = ClientOptions::from_env()?;
    run_with_options(options)
}

pub fn run_with_options(options: ClientOptions) -> Result<(), ClientError> {
    let print_options = options.print_options.clone();
    let address = (options.host.as_str(), options.port)
        .to_socket_addrs()
        .map_err(ClientError::Io)?
        .next()
        .ok_or_else(|| ClientError::InvalidArgs {
            reason: "host resolved to no socket addresses".to_owned(),
        })?;

    let stream = TcpStream::connect(address).map_err(ClientError::Io)?;
    stream.set_nodelay(true).ok();
    let mut session = PgWireSession::new(stream);
    session.startup(&options.user, Some(&options.database))?;

    if let Some(query) = options.query {
        execute_and_render(&mut session, &query, &print_options)?;
        session.terminate()?;
        return Ok(());
    }

    if io::stdin().is_terminal() {
        interactive_session(&mut session, print_options)?;
    } else {
        let mut sql = String::new();
        io::stdin().read_to_string(&mut sql)?;
        if !sql.trim().is_empty() {
            execute_and_render(&mut session, sql.trim(), &print_options)?;
        }
        session.terminate()?;
        return Ok(());
    }

    session.terminate()?;
    Ok(())
}

pub struct PgWireSession<S> {
    stream: S,
}

impl<S: Read + Write> PgWireSession<S> {
    pub fn new(stream: S) -> Self {
        Self { stream }
    }

    pub fn startup(&mut self, user: &str, database: Option<&str>) -> Result<(), ClientError> {
        write_startup_message(&mut self.stream, user, database)?;
        self.read_until_ready()
    }

    pub fn execute(&mut self, sql: &str) -> Result<QueryResult, ClientError> {
        write_query_message(&mut self.stream, sql)?;
        self.read_query_response()
    }

    pub fn terminate(&mut self) -> Result<(), ClientError> {
        write_terminate_message(&mut self.stream)
    }

    fn read_until_ready(&mut self) -> Result<(), ClientError> {
        loop {
            let (message_type, payload) = read_frame(&mut self.stream)?;
            match message_type {
                b'R' => {
                    if payload.len() != 4 {
                        return Err(ClientError::Protocol {
                            reason: "authentication response had an invalid payload".to_owned(),
                        });
                    }
                    let code = i32::from_be_bytes(payload.as_slice().try_into().unwrap());
                    if code != 0 {
                        return Err(ClientError::Protocol {
                            reason: format!("unsupported authentication method {code}"),
                        });
                    }
                }
                b'Z' => return Ok(()),
                b'N' => {}
                b'E' => {
                    let error = parse_error(&payload);
                    return Err(ClientError::Server {
                        reason: error.message,
                        error_code: error.code,
                    });
                }
                other => {
                    return Err(ClientError::Protocol {
                        reason: format!("unexpected startup message `{}`", other as char),
                    });
                }
            }
        }
    }

    fn read_query_response(&mut self) -> Result<QueryResult, ClientError> {
        let mut headers = Vec::new();
        let mut rows = Vec::new();
        let mut command_tag = String::new();
        let mut server_error = None;

        loop {
            let (message_type, payload) = read_frame(&mut self.stream)?;
            match message_type {
                b'T' => headers = parse_row_description(&payload)?,
                b'D' => rows.push(parse_data_row(&payload)?),
                b'C' => command_tag = parse_cstring(&payload)?,
                b'N' => {}
                b'E' => server_error = Some(parse_error(&payload)),
                b'Z' => break,
                other => {
                    return Err(ClientError::Protocol {
                        reason: format!("unexpected query message `{}`", other as char),
                    });
                }
            }
        }

        if let Some(error) = server_error {
            return Err(ClientError::Server {
                reason: error.message,
                error_code: error.code,
            });
        }

        Ok(QueryResult {
            headers,
            rows,
            command_tag,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub command_tag: String,
}

pub fn render_query_result(mut out: impl Write, result: &QueryResult) -> Result<(), ClientError> {
    if !result.headers.is_empty() {
        render_table(&mut out, result)?;
        writeln!(out)?;
    }
    if !result.command_tag.is_empty() {
        writeln!(out, "{}", result.command_tag)?;
    }
    Ok(())
}

fn render_table(out: &mut impl Write, result: &QueryResult) -> Result<(), ClientError> {
    let mut widths: Vec<usize> = result.headers.iter().map(|header| header.len()).collect();
    for row in &result.rows {
        for (idx, value) in row.iter().enumerate() {
            let text = value.as_deref().unwrap_or("NULL");
            if idx >= widths.len() {
                widths.push(text.len());
            } else {
                widths[idx] = widths[idx].max(text.len());
            }
        }
    }

    write_border(out, &widths)?;
    write_row(out, &result.headers, &widths)?;
    write_border(out, &widths)?;
    for row in &result.rows {
        let values = row
            .iter()
            .map(|value| value.as_deref().unwrap_or("NULL").to_owned())
            .collect::<Vec<_>>();
        write_row(out, &values, &widths)?;
    }
    write_border(out, &widths)?;
    writeln!(out, "{} rows", result.rows.len())?;
    Ok(())
}

fn write_border(out: &mut impl Write, widths: &[usize]) -> Result<(), ClientError> {
    write!(out, "+")?;
    for width in widths {
        write!(out, "{}+", "-".repeat(*width + 2))?;
    }
    writeln!(out)?;
    Ok(())
}

fn write_row(out: &mut impl Write, values: &[String], widths: &[usize]) -> Result<(), ClientError> {
    write!(out, "|")?;
    for (idx, width) in widths.iter().enumerate() {
        let value = values.get(idx).map(String::as_str).unwrap_or("");
        write!(out, " {:<width$} |", value, width = *width)?;
    }
    writeln!(out)?;
    Ok(())
}

fn interactive_session<S: Read + Write>(
    session: &mut PgWireSession<S>,
    mut print_options: PrintOptions,
) -> Result<(), ClientError> {
    let mut line_editor = DefaultEditor::new().map_err(|error| ClientError::LineEditor {
        reason: format!("line editor failed to start: {error}"),
    })?;
    if let Some(path) = history_path() {
        let _ = line_editor.load_history(&path);
    }
    let mut buffer = String::new();

    loop {
        let prompt = if buffer.trim().is_empty() {
            "brewdb> "
        } else {
            "   ...> "
        };
        let line = match line_editor.readline(prompt) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                writeln!(io::stdout())?;
                break;
            }
            Err(ReadlineError::Eof) => break,
            Err(error) => {
                return Err(ClientError::LineEditor {
                    reason: format!("line editor failed: {error}"),
                });
            }
        };

        let trimmed = line.trim();
        if is_quit_command(trimmed) {
            break;
        }
        if buffer.trim().is_empty() && trimmed.starts_with('\\') {
            match trimmed.parse::<Command>() {
                Ok(command) => {
                    let mut stdout = io::stdout();
                    match command.execute(session, &mut print_options, &mut stdout) {
                        Ok(CommandAction::Continue) => {}
                        Ok(CommandAction::Quit) => break,
                        Err(error) => eprintln!("brewdb failed: {error}"),
                    }
                }
                Err(error) => eprintln!("brewdb failed: {error}"),
            }
            continue;
        }
        if trimmed.is_empty() {
            if !buffer.trim().is_empty() {
                if let Err(error) = execute_and_render(session, buffer.trim(), &print_options) {
                    eprintln!("brewdb failed: {error}");
                }
                buffer.clear();
            }
            continue;
        }

        let _ = line_editor.add_history_entry(line.as_str());
        buffer.push_str(&line);
        buffer.push('\n');
        if trimmed.ends_with(';') {
            let sql = buffer.trim().trim_end_matches(';').trim().to_owned();
            if !sql.is_empty() {
                if let Err(error) = execute_and_render(session, &sql, &print_options) {
                    eprintln!("brewdb failed: {error}");
                }
            }
            buffer.clear();
        }
    }

    if !buffer.trim().is_empty() {
        if let Err(error) = execute_and_render(session, buffer.trim(), &print_options) {
            eprintln!("brewdb failed: {error}");
        }
    }

    if let Some(path) = history_path() {
        let _ = line_editor.save_history(&path);
    }

    Ok(())
}

fn history_path() -> Option<PathBuf> {
    env::var_os("HOME").map(|home| PathBuf::from(home).join(".brewdb_history"))
}

fn execute_and_render<S: Read + Write>(
    session: &mut PgWireSession<S>,
    sql: &str,
    print_options: &PrintOptions,
) -> Result<(), ClientError> {
    let mut stdout = io::stdout();
    execute_and_render_to(session, sql, print_options, &mut stdout)
}

pub(crate) fn execute_and_render_to<S: Read + Write>(
    session: &mut PgWireSession<S>,
    sql: &str,
    print_options: &PrintOptions,
    out: &mut impl Write,
) -> Result<(), ClientError> {
    let started_at = Instant::now();
    let result = session.execute(sql)?;
    print_options.print_query_result(out, &result, started_at)
}

fn write_startup_message<S: Write>(
    stream: &mut S,
    user: &str,
    database: Option<&str>,
) -> Result<(), ClientError> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&196_608_i32.to_be_bytes());
    payload.extend_from_slice(b"user\0");
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    if let Some(database) = database {
        payload.extend_from_slice(b"database\0");
        payload.extend_from_slice(database.as_bytes());
        payload.push(0);
    }
    payload.push(0);

    let length = (payload.len() + 4) as i32;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(&payload)?;
    stream.flush()?;
    Ok(())
}

fn write_query_message<S: Write>(stream: &mut S, sql: &str) -> Result<(), ClientError> {
    let mut payload = sql.as_bytes().to_vec();
    payload.push(0);
    write_frame(stream, b'Q', &payload)
}

fn write_terminate_message<S: Write>(stream: &mut S) -> Result<(), ClientError> {
    write_frame(stream, b'X', &[])
}

fn write_frame<S: Write>(
    stream: &mut S,
    message_type: u8,
    payload: &[u8],
) -> Result<(), ClientError> {
    let length = (payload.len() + 4) as i32;
    stream.write_all(&[message_type])?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(payload)?;
    stream.flush()?;
    Ok(())
}

fn read_frame<S: Read>(stream: &mut S) -> Result<(u8, Vec<u8>), ClientError> {
    let mut message_type = [0_u8; 1];
    stream.read_exact(&mut message_type)?;
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length)?;
    let length = i32::from_be_bytes(length);
    if length < 4 {
        return Err(ClientError::Protocol {
            reason: "frame length was too small".to_owned(),
        });
    }
    let mut payload = vec![0; length as usize - 4];
    stream.read_exact(&mut payload)?;
    Ok((message_type[0], payload))
}

fn parse_row_description(payload: &[u8]) -> Result<Vec<String>, ClientError> {
    let mut offset = 0;
    let field_count = read_i16(payload, &mut offset)? as usize;
    let mut fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        let (name, next) = read_cstring_at(payload, offset)?;
        offset = next;
        offset = offset
            .checked_add(18)
            .ok_or_else(|| ClientError::Protocol {
                reason: "row description overflowed".to_owned(),
            })?;
        if offset > payload.len() {
            return Err(ClientError::Protocol {
                reason: "row description payload was truncated".to_owned(),
            });
        }
        fields.push(name);
    }
    Ok(fields)
}

fn parse_data_row(payload: &[u8]) -> Result<Vec<Option<String>>, ClientError> {
    let mut offset = 0;
    let column_count = read_i16(payload, &mut offset)? as usize;
    let mut row = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let len = read_i32(payload, &mut offset)?;
        if len < 0 {
            row.push(None);
            continue;
        }
        let len = len as usize;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| ClientError::Protocol {
                reason: "data row overflowed".to_owned(),
            })?;
        if end > payload.len() {
            return Err(ClientError::Protocol {
                reason: "data row payload was truncated".to_owned(),
            });
        }
        let value = std::str::from_utf8(&payload[offset..end])
            .map_err(|_| ClientError::Protocol {
                reason: "data row value was not valid UTF-8".to_owned(),
            })?
            .to_owned();
        row.push(Some(value));
        offset = end;
    }
    Ok(row)
}

fn parse_cstring(payload: &[u8]) -> Result<String, ClientError> {
    let (value, next) = read_cstring_at(payload, 0)?;
    if next != payload.len() {
        return Err(ClientError::Protocol {
            reason: "payload contained trailing bytes".to_owned(),
        });
    }
    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerErrorFields {
    message: String,
    code: Option<String>,
}

fn parse_error(payload: &[u8]) -> ServerErrorFields {
    let mut offset = 0;
    let mut message = None;
    let mut code = None;
    while offset < payload.len() {
        let field_type = payload[offset];
        offset += 1;
        if field_type == 0 {
            break;
        }
        let start = offset;
        while offset < payload.len() && payload[offset] != 0 {
            offset += 1;
        }
        let value = std::str::from_utf8(&payload[start..offset]).unwrap_or("");
        match field_type {
            b'M' => message = Some(value.to_owned()),
            b'C' => code = Some(value.to_owned()),
            _ => {}
        }
        offset += 1;
    }
    ServerErrorFields {
        message: message.unwrap_or_else(|| "server returned an error".to_owned()),
        code,
    }
}

fn read_i16(payload: &[u8], offset: &mut usize) -> Result<i16, ClientError> {
    let end = offset.checked_add(2).ok_or_else(|| ClientError::Protocol {
        reason: "integer read overflowed".to_owned(),
    })?;
    if end > payload.len() {
        return Err(ClientError::Protocol {
            reason: "payload was truncated".to_owned(),
        });
    }
    let value = i16::from_be_bytes(payload[*offset..end].try_into().unwrap());
    *offset = end;
    Ok(value)
}

fn read_i32(payload: &[u8], offset: &mut usize) -> Result<i32, ClientError> {
    let end = offset.checked_add(4).ok_or_else(|| ClientError::Protocol {
        reason: "integer read overflowed".to_owned(),
    })?;
    if end > payload.len() {
        return Err(ClientError::Protocol {
            reason: "payload was truncated".to_owned(),
        });
    }
    let value = i32::from_be_bytes(payload[*offset..end].try_into().unwrap());
    *offset = end;
    Ok(value)
}

fn read_cstring_at(payload: &[u8], offset: usize) -> Result<(String, usize), ClientError> {
    let end = payload[offset..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|position| offset + position)
        .ok_or_else(|| ClientError::Protocol {
            reason: "unterminated string".to_owned(),
        })?;
    let value = std::str::from_utf8(&payload[offset..end])
        .map_err(|_| ClientError::Protocol {
            reason: "string was not valid UTF-8".to_owned(),
        })?
        .to_owned();
    Ok((value, end + 1))
}

fn take_value<I>(flag: &str, args: &mut std::iter::Peekable<I>) -> Result<String, ClientError>
where
    I: Iterator<Item = String>,
{
    args.next().ok_or_else(|| ClientError::InvalidArgs {
        reason: format!("missing value for `{flag}`"),
    })
}

fn usage() -> String {
    "usage: brewdb [--host HOST] [--port PORT] [--user USER] [--database DB] [--quiet] [--maxrows N] [-c SQL]".to_owned()
}

fn is_quit_command(command: &str) -> bool {
    matches!(command.trim(), r"\q" | "quit;" | "exit;")
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::os::unix::net::UnixStream;
    use std::sync::mpsc;
    use std::thread;

    use crate::print_options::MaxRows;

    use super::*;

    #[test]
    fn args_parse_supports_positional_sql() {
        let options = ClientOptions::from_args(["-h", "localhost", "select 1"]).unwrap();
        assert_eq!(options.host, "localhost");
        assert_eq!(options.query.as_deref(), Some("select 1"));
    }

    #[test]
    fn args_parse_print_options() {
        let options = ClientOptions::from_args(["--quiet", "--maxrows", "10", "select 1"]).unwrap();

        assert!(options.print_options.quiet);
        assert_eq!(options.print_options.maxrows, MaxRows::Limited(10));
        assert_eq!(options.query.as_deref(), Some("select 1"));
    }

    #[test]
    fn quit_command_accepts_common_spellings() {
        assert!(is_quit_command(r"\q"));
        assert!(is_quit_command("quit;"));
        assert!(is_quit_command("exit;"));
        assert!(!is_quit_command("quit"));
        assert!(!is_quit_command("exit"));
        assert!(!is_quit_command("select 1"));
    }

    #[test]
    fn client_runs_simple_query_against_fake_server() {
        let (server, client) = UnixStream::pair().unwrap();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut server = server;
            let _ = handle_fake_server(&mut server, tx);
        });

        let mut session = PgWireSession::new(client);
        session.startup("brew", Some("brewdb")).unwrap();
        let result = session.execute("select 1").unwrap();

        assert_eq!(result.headers, vec!["id"]);
        assert_eq!(result.rows, vec![vec![Some("1".to_owned())]]);
        assert_eq!(result.command_tag, "SELECT 1");
        assert_eq!(rx.recv().unwrap(), "select 1");
    }

    #[test]
    fn execute_and_render_uses_supplied_print_options() {
        let (server, client) = UnixStream::pair().unwrap();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut server = server;
            let _ = handle_fake_server(&mut server, tx);
        });

        let mut session = PgWireSession::new(client);
        session.startup("brew", Some("brewdb")).unwrap();
        let mut out = Vec::new();
        let print_options = PrintOptions {
            quiet: true,
            maxrows: MaxRows::Unlimited,
        };

        execute_and_render_to(&mut session, "select 1", &print_options, &mut out).unwrap();

        let output = String::from_utf8(out).unwrap();
        assert!(output.contains("SELECT 1"));
        assert!(!output.contains("Elapsed"));
        assert_eq!(rx.recv().unwrap(), "select 1");
    }

    #[test]
    fn parses_repl_meta_commands() {
        assert_eq!(
            r"\q".parse::<crate::command::Command>().unwrap(),
            crate::command::Command::Quit
        );
        assert_eq!(
            r"\quiet true".parse::<crate::command::Command>().unwrap(),
            crate::command::Command::QuietMode(Some(true))
        );
        assert!(r"\d hits".parse::<crate::command::Command>().is_err());
    }

    fn handle_fake_server(
        stream: &mut UnixStream,
        tx: mpsc::Sender<String>,
    ) -> Result<(), Box<dyn Error>> {
        read_startup_packet(stream)?;
        write_frame(stream, b'R', &0_i32.to_be_bytes())?;
        write_frame(stream, b'Z', b"I")?;

        let (message_type, payload) = read_frame(stream)?;
        assert_eq!(message_type, b'Q');
        tx.send(parse_cstring(&payload)?).unwrap();

        let mut row_desc = Vec::new();
        row_desc.extend_from_slice(&1_i16.to_be_bytes());
        row_desc.extend_from_slice(b"id\0");
        row_desc.extend_from_slice(&0_i32.to_be_bytes());
        row_desc.extend_from_slice(&0_i16.to_be_bytes());
        row_desc.extend_from_slice(&23_i32.to_be_bytes());
        row_desc.extend_from_slice(&(-1_i16).to_be_bytes());
        row_desc.extend_from_slice(&0_i32.to_be_bytes());
        row_desc.extend_from_slice(&0_i16.to_be_bytes());
        write_frame(stream, b'T', &row_desc)?;

        let mut row = Vec::new();
        row.extend_from_slice(&1_i16.to_be_bytes());
        row.extend_from_slice(&1_i32.to_be_bytes());
        row.extend_from_slice(b"1");
        write_frame(stream, b'D', &row)?;

        write_frame(stream, b'C', b"SELECT 1\0")?;
        write_frame(stream, b'Z', b"I")?;
        Ok(())
    }

    fn read_startup_packet(stream: &mut UnixStream) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut length = [0_u8; 4];
        stream.read_exact(&mut length)?;
        let length = i32::from_be_bytes(length);
        let mut payload = vec![0; length as usize - 4];
        stream.read_exact(&mut payload)?;
        Ok(payload)
    }
}
