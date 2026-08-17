//! Client-facing result shaping contracts.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultField {
    pub name: String,
    pub data_type: String,
}

impl ResultField {
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryResultKind {
    Query,
    Command,
    Empty,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandTag(String);

impl CommandTag {
    pub fn new(tag: impl Into<String>) -> Self {
        Self(tag.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryResultOutput {
    pub kind: QueryResultKind,
    pub command_tag: CommandTag,
    pub row_count: u64,
    pub fields: Vec<ResultField>,
}

impl QueryResultOutput {
    pub fn query(command_tag: impl Into<String>, row_count: u64, fields: Vec<ResultField>) -> Self {
        Self {
            kind: QueryResultKind::Query,
            command_tag: CommandTag::new(command_tag),
            row_count,
            fields,
        }
    }

    pub fn command(command_tag: impl Into<String>) -> Self {
        Self {
            kind: QueryResultKind::Command,
            command_tag: CommandTag::new(command_tag),
            row_count: 0,
            fields: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notice {
    pub severity: &'static str,
    pub message: String,
}

impl Notice {
    pub fn info(message: impl Into<String>) -> Self {
        Self {
            severity: "INFO",
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontendResponse {
    pub result: QueryResultOutput,
    pub notices: Vec<Notice>,
}

impl FrontendResponse {
    pub fn new(result: QueryResultOutput) -> Self {
        Self {
            result,
            notices: Vec::new(),
        }
    }

    pub fn with_notice(mut self, notice: Notice) -> Self {
        self.notices.push(notice);
        self
    }
}
