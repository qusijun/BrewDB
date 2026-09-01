// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Commands handled by the interactive CLI before SQL is sent to BrewDB.

use std::fmt;
use std::fs;
use std::io::{Read, Write};
use std::str::FromStr;

use crate::client::{ClientError, PgWireSession, execute_and_render_to};
use crate::print_options::PrintOptions;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Quit,
    Help,
    Include(Option<String>),
    QuietMode(Option<bool>),
    OutputFormat(Option<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandAction {
    Continue,
    Quit,
}

impl Command {
    pub fn execute<S: Read + Write, W: Write>(
        &self,
        session: &mut PgWireSession<S>,
        print_options: &mut PrintOptions,
        out: &mut W,
    ) -> Result<CommandAction, ClientError> {
        match self {
            Self::Quit => Ok(CommandAction::Quit),
            Self::Help => {
                write_help(out)?;
                Ok(CommandAction::Continue)
            }
            Self::Include(filename) => {
                let filename = filename.as_ref().ok_or_else(|| ClientError::InvalidArgs {
                    reason: "Required filename argument is missing".to_owned(),
                })?;
                let sql = fs::read_to_string(filename)?;
                for statement in sql.split(';').map(str::trim).filter(|sql| !sql.is_empty()) {
                    execute_and_render_to(session, statement, print_options, out)?;
                }
                Ok(CommandAction::Continue)
            }
            Self::QuietMode(quiet) => {
                if let Some(quiet) = quiet {
                    print_options.quiet = *quiet;
                    writeln!(
                        out,
                        "Quiet mode set to {}",
                        if print_options.quiet { "true" } else { "false" }
                    )?;
                } else {
                    writeln!(
                        out,
                        "Quiet mode is {}",
                        if print_options.quiet { "true" } else { "false" }
                    )?;
                }
                Ok(CommandAction::Continue)
            }
            Self::OutputFormat(_) => Err(ClientError::InvalidArgs {
                reason: "changing output format is not supported yet".to_owned(),
            }),
        }
    }

    fn get_name_and_description(&self) -> (&'static str, &'static str) {
        match self {
            Self::Quit => ("\\q", "quit brewdb"),
            Self::Help => ("\\?", "help"),
            Self::Include(_) => ("\\i filename", "reads input from the specified filename"),
            Self::QuietMode(_) => ("\\quiet (true|false)?", "print or set quiet mode"),
            Self::OutputFormat(_) => ("\\pset [NAME [VALUE]]", "set table output option"),
        }
    }
}

const ALL_COMMANDS: [Command; 5] = [
    Command::Quit,
    Command::Help,
    Command::Include(Some(String::new())),
    Command::QuietMode(None),
    Command::OutputFormat(None),
];

fn write_help(out: &mut impl Write) -> Result<(), ClientError> {
    writeln!(out, "Available commands:")?;
    for command in ALL_COMMANDS {
        let (name, description) = command.get_name_and_description();
        writeln!(out, "{name:<24} {description}")?;
    }
    Ok(())
}

impl FromStr for Command {
    type Err = ParseCommandError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().strip_prefix('\\').unwrap_or(s.trim());
        let (command, arg) = if let Some((command, arg)) = s.split_once(' ') {
            (command, Some(arg.trim()))
        } else {
            (s, None)
        };
        Ok(match (command, arg) {
            ("q", None) => Self::Quit,
            ("?", None) => Self::Help,
            ("i", None) => Self::Include(None),
            ("i", Some(filename)) if !filename.is_empty() => {
                Self::Include(Some(filename.to_owned()))
            }
            ("quiet", Some("true" | "t" | "yes" | "y" | "on")) => Self::QuietMode(Some(true)),
            ("quiet", Some("false" | "f" | "no" | "n" | "off")) => Self::QuietMode(Some(false)),
            ("quiet", None) => Self::QuietMode(None),
            ("pset", Some(subcommand)) => Self::OutputFormat(Some(subcommand.to_owned())),
            ("pset", None) => Self::OutputFormat(None),
            _ => return Err(ParseCommandError),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseCommandError;

impl fmt::Display for ParseCommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid command")
    }
}

impl std::error::Error for ParseCommandError {}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixStream;

    use super::*;
    use crate::print_options::MaxRows;

    #[test]
    fn command_parse_supports_datafusion_cli_commands() {
        assert_eq!(r"\q".parse::<Command>().unwrap(), Command::Quit);
        assert_eq!(r"\?".parse::<Command>().unwrap(), Command::Help);
        assert_eq!(
            r"\i setup.sql".parse::<Command>().unwrap(),
            Command::Include(Some("setup.sql".to_owned()))
        );
        assert_eq!(
            r"\quiet on".parse::<Command>().unwrap(),
            Command::QuietMode(Some(true))
        );
        assert_eq!(
            r"\quiet off".parse::<Command>().unwrap(),
            Command::QuietMode(Some(false))
        );
        assert_eq!(
            r"\pset format table".parse::<Command>().unwrap(),
            Command::OutputFormat(Some("format table".to_owned()))
        );
    }

    #[test]
    fn command_parse_rejects_unknown_commands() {
        assert!(r"\unknown".parse::<Command>().is_err());
        assert!(r"\d".parse::<Command>().is_err());
        assert!(r"\d hits".parse::<Command>().is_err());
    }

    #[test]
    fn quiet_command_mutates_print_options() {
        let (_server, client) = UnixStream::pair().unwrap();
        let mut session = PgWireSession::new(client);
        let mut print_options = PrintOptions {
            quiet: false,
            maxrows: MaxRows::Unlimited,
        };
        let mut out = Vec::new();

        Command::QuietMode(Some(true))
            .execute(&mut session, &mut print_options, &mut out)
            .unwrap();

        assert!(print_options.quiet);
        assert_eq!(String::from_utf8(out).unwrap(), "Quiet mode set to true\n");
    }
}
