//! REST transport shell for Lakekeeper-backed control-plane access.

use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use url::Url;

use brewdb_core::ids::{TableId, WarehouseId};

use crate::client::CatalogClient;
use crate::errors::CatalogError;
use crate::model::{TableRecord, WarehouseProfile};

/// Stable API families exposed by Lakekeeper.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LakekeeperApiRoute {
    Catalog,
    Management,
    Data,
}

impl LakekeeperApiRoute {
    pub const fn path_prefix(self) -> &'static str {
        match self {
            Self::Catalog => "/catalog/v1",
            Self::Management => "/management/v1",
            Self::Data => "/lakekeeper",
        }
    }
}

/// Runtime config for connecting BrewDB catalog facade to Lakekeeper.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LakekeeperClientConfig {
    pub base_uri: String,
    pub project_id: Option<String>,
    pub warehouse_name: Option<String>,
    pub bearer_token: Option<String>,
}

impl LakekeeperClientConfig {
    pub fn parse_base_url(&self) -> Result<Url, CatalogError> {
        Url::parse(&self.base_uri).map_err(|reason| CatalogError::Unsupported {
            operation: "lakekeeper_base_uri",
            reason: reason.to_string(),
        })
    }
}

/// Reqwest-backed REST client shell.
#[derive(Clone, Debug)]
pub struct LakekeeperRestCatalogClient {
    pub http_client: HttpClient,
    pub config: LakekeeperClientConfig,
}

impl LakekeeperRestCatalogClient {
    pub fn new(config: LakekeeperClientConfig) -> Self {
        Self {
            http_client: HttpClient::new(),
            config,
        }
    }

    pub fn api_base_url(&self, route: LakekeeperApiRoute) -> Result<Url, CatalogError> {
        self.config
            .parse_base_url()?
            .join(route.path_prefix().trim_start_matches('/'))
            .map_err(|reason| CatalogError::Unsupported {
                operation: "lakekeeper_route_join",
                reason: reason.to_string(),
            })
    }
}

impl CatalogClient for LakekeeperRestCatalogClient {
    fn fetch_table(&self, _table_id: &TableId) -> Result<TableRecord, CatalogError> {
        Err(CatalogError::Unsupported {
            operation: "lakekeeper.rest.fetch_table",
            reason: "Lakekeeper REST transport integration is not implemented yet".to_owned(),
        })
    }

    fn fetch_warehouse_profile(
        &self,
        _warehouse_id: &WarehouseId,
    ) -> Result<WarehouseProfile, CatalogError> {
        Err(CatalogError::Unsupported {
            operation: "lakekeeper.rest.fetch_warehouse_profile",
            reason: "Lakekeeper REST transport integration is not implemented yet".to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{LakekeeperApiRoute, LakekeeperClientConfig, LakekeeperRestCatalogClient};

    #[test]
    fn rest_client_builds_management_route_base() {
        let client = LakekeeperRestCatalogClient::new(LakekeeperClientConfig {
            base_uri: "https://lakekeeper.example.com/".to_owned(),
            project_id: Some("proj-a".to_owned()),
            warehouse_name: Some("warehouse-a".to_owned()),
            bearer_token: None,
        });

        let url = client.api_base_url(LakekeeperApiRoute::Management).unwrap();

        assert_eq!(url.as_str(), "https://lakekeeper.example.com/management/v1");
    }

    #[test]
    fn rest_client_builds_catalog_route_base() {
        let client = LakekeeperRestCatalogClient::new(LakekeeperClientConfig {
            base_uri: "https://lakekeeper.example.com".to_owned(),
            project_id: None,
            warehouse_name: None,
            bearer_token: Some("token-a".to_owned()),
        });

        let url = client.api_base_url(LakekeeperApiRoute::Catalog).unwrap();

        assert_eq!(url.as_str(), "https://lakekeeper.example.com/catalog/v1");
    }
}
