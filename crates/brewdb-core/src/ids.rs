//! Shared BrewDB identifiers.

use std::fmt;
use std::str::FromStr;

pub use uuid::Uuid;

macro_rules! uuid_id {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(Uuid);

        impl $name {
            pub fn new(value: Uuid) -> Self {
                Self(value)
            }

            pub fn generate() -> Self {
                Self(Uuid::new_v4())
            }

            pub fn parse_str(input: &str) -> Result<Self, uuid::Error> {
                Ok(Self(Uuid::parse_str(input)?))
            }

            pub fn as_uuid(&self) -> &Uuid {
                &self.0
            }

            pub fn into_inner(self) -> Uuid {
                self.0
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.into_inner()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::parse_str(s)
            }
        }
    };
}

uuid_id!(RequestId);
uuid_id!(SessionId);
uuid_id!(NamespaceId);
uuid_id!(TableId);
uuid_id!(WarehouseId);
uuid_id!(JobId);
uuid_id!(StageId);
uuid_id!(TaskId);
uuid_id!(TaskAttemptId);
uuid_id!(TxnId);
uuid_id!(CommitAttemptId);
uuid_id!(ArtifactId);

#[cfg(test)]
mod tests {
    use super::{JobId, TxnId, Uuid};

    #[test]
    fn ids_preserve_uuid_values() {
        let job_id = JobId::generate();
        let txn_id = TxnId::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        assert_eq!(job_id.to_string().len(), 36);
        assert_eq!(txn_id.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn generated_uuid_has_v4_shape() {
        let uuid = Uuid::new_v4();

        assert_eq!(uuid.to_string().chars().nth(14), Some('4'));
        assert!(matches!(
            uuid.to_string().chars().nth(19),
            Some('8' | '9' | 'a' | 'b')
        ));
    }
}
