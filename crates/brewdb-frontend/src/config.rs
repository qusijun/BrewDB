//! Frontend configuration view.

use crate::common::defaults::MANAGED_PAIMON_CATALOG_NAME;
use crate::common::errors::CommonError;

pub const PGWIRE_LISTEN_ADDR_KEY: &str = "brewdb.frontend.pgwire.listen_addr";
pub const DEFAULT_CATALOG_KEY: &str = "brewdb.frontend.default_catalog";
brewdb_common::define_config_view! {
    pub struct FrontendConfig {
        default_catalog: String {
            key: DEFAULT_CATALOG_KEY,
            kind: String,
            default: MANAGED_PAIMON_CATALOG_NAME,
            scopes: [crate::common::config::ConfigScope::System],
            parse: |value: &str| Ok(value.to_owned()),
        },
        pgwire_listen_addr: String {
            key: PGWIRE_LISTEN_ADDR_KEY,
            kind: String,
            default: "127.0.0.1:5432",
            scopes: [crate::common::config::ConfigScope::System],
            parse: |value: &str| -> Result<String, CommonError> {
                value.parse::<std::net::SocketAddr>()
                    .map(|_| value.to_owned())
                    .map_err(|error| CommonError::InvalidConfiguration {
                        field: PGWIRE_LISTEN_ADDR_KEY.to_owned(),
                        reason: format!("invalid socket address: {error}"),
                    })
            },
        },
    }
}

impl FrontendConfig {
    pub fn default_catalog(&self) -> &str {
        &self.default_catalog
    }

    pub fn pgwire_listen_addr(&self) -> &str {
        &self.pgwire_listen_addr
    }
}
