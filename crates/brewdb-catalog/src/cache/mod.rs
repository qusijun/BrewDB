//! Catalog cache boundary.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::model::{
    CatalogEntry, CatalogRef, DatabaseEntry, DatabaseRef, TableCatalogEntry, TableRef,
};
use crate::path::{CatalogPath, DatabasePath, TablePath};

pub trait CatalogCache: Send + Sync {
    fn get_catalog(&self, path: &CatalogPath) -> Option<CatalogEntry>;

    fn get_catalog_by_ref(&self, catalog_ref: CatalogRef) -> Option<CatalogEntry>;

    fn get_database(&self, path: &DatabasePath) -> Option<DatabaseEntry>;

    fn get_database_by_ref(&self, database_ref: DatabaseRef) -> Option<DatabaseEntry>;

    fn get_table(&self, path: &TablePath) -> Option<TableCatalogEntry>;

    fn get_table_by_ref(&self, table_ref: TableRef) -> Option<TableCatalogEntry>;

    fn put_catalog(&self, entry: CatalogEntry);

    fn put_database(&self, entry: DatabaseEntry);

    fn put_table(&self, entry: TableCatalogEntry);

    fn invalidate_catalog(&self, path: &CatalogPath);

    fn invalidate_database(&self, path: &DatabasePath);

    fn invalidate_table(&self, path: &TablePath);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheEvictionPolicy {
    Lru,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogCacheConfig {
    pub enabled: bool,
    pub capacity: usize,
    pub negative_capacity: usize,
    pub eviction_policy: CacheEvictionPolicy,
}

impl Default for CatalogCacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            capacity: 10_000,
            negative_capacity: 1_000,
            eviction_policy: CacheEvictionPolicy::Lru,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CatalogCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub invalidations: u64,
}

pub trait CatalogCacheManager: Send + Sync {
    fn cache(&self) -> &Arc<dyn CatalogCache>;

    fn config(&self) -> &CatalogCacheConfig;

    fn stats(&self) -> CatalogCacheStats;

    fn record_hit(&self);

    fn record_miss(&self);

    fn record_eviction(&self);

    fn invalidate_catalog(&self, path: &CatalogPath);

    fn invalidate_database(&self, path: &DatabasePath);

    fn invalidate_table(&self, path: &TablePath);
}

pub struct DefaultCatalogCacheManager {
    cache: Arc<dyn CatalogCache>,
    config: CatalogCacheConfig,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    invalidations: AtomicU64,
}

impl DefaultCatalogCacheManager {
    pub fn new(cache: Arc<dyn CatalogCache>, config: CatalogCacheConfig) -> Self {
        Self {
            cache,
            config,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
        }
    }
}

impl CatalogCacheManager for DefaultCatalogCacheManager {
    fn cache(&self) -> &Arc<dyn CatalogCache> {
        &self.cache
    }

    fn config(&self) -> &CatalogCacheConfig {
        &self.config
    }

    fn stats(&self) -> CatalogCacheStats {
        CatalogCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
        }
    }

    fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    fn invalidate_catalog(&self, path: &CatalogPath) {
        self.cache.invalidate_catalog(path);
        self.invalidations.fetch_add(1, Ordering::Relaxed);
    }

    fn invalidate_database(&self, path: &DatabasePath) {
        self.cache.invalidate_database(path);
        self.invalidations.fetch_add(1, Ordering::Relaxed);
    }

    fn invalidate_table(&self, path: &TablePath) {
        self.cache.invalidate_table(path);
        self.invalidations.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub struct NoopCatalogCache;

impl CatalogCache for NoopCatalogCache {
    fn get_catalog(&self, _path: &CatalogPath) -> Option<CatalogEntry> {
        None
    }

    fn get_catalog_by_ref(&self, _catalog_ref: CatalogRef) -> Option<CatalogEntry> {
        None
    }

    fn get_database(&self, _path: &DatabasePath) -> Option<DatabaseEntry> {
        None
    }

    fn get_database_by_ref(&self, _database_ref: DatabaseRef) -> Option<DatabaseEntry> {
        None
    }

    fn get_table(&self, _path: &TablePath) -> Option<TableCatalogEntry> {
        None
    }

    fn get_table_by_ref(&self, _table_ref: TableRef) -> Option<TableCatalogEntry> {
        None
    }

    fn put_catalog(&self, _entry: CatalogEntry) {}

    fn put_database(&self, _entry: DatabaseEntry) {}

    fn put_table(&self, _entry: TableCatalogEntry) {}

    fn invalidate_catalog(&self, _path: &CatalogPath) {}

    fn invalidate_database(&self, _path: &DatabasePath) {}

    fn invalidate_table(&self, _path: &TablePath) {}
}

pub fn new_noop_cache_manager() -> DefaultCatalogCacheManager {
    DefaultCatalogCacheManager::new(Arc::new(NoopCatalogCache), CatalogCacheConfig::default())
}

#[cfg(test)]
mod tests {
    use super::{
        CacheEvictionPolicy, CatalogCacheConfig, CatalogCacheManager, CatalogCacheStats,
        new_noop_cache_manager,
    };
    use crate::path::{CatalogPath, DatabasePath, TablePath};

    #[test]
    fn noop_cache_manager_tracks_stats_and_invalidations() {
        let manager = new_noop_cache_manager();

        manager.record_hit();
        manager.record_miss();
        manager.record_eviction();
        manager.invalidate_catalog(&CatalogPath::new("prod").unwrap());
        manager.invalidate_database(&DatabasePath::new("prod", "sales").unwrap());
        manager.invalidate_table(&TablePath::new("prod", "sales", "orders").unwrap());

        assert_eq!(
            manager.stats(),
            CatalogCacheStats {
                hits: 1,
                misses: 1,
                evictions: 1,
                invalidations: 3,
            }
        );
    }

    #[test]
    fn cache_config_defaults_are_explicit() {
        let config = CatalogCacheConfig::default();

        assert_eq!(
            config,
            CatalogCacheConfig {
                enabled: false,
                capacity: 10_000,
                negative_capacity: 1_000,
                eviction_policy: CacheEvictionPolicy::Lru,
            }
        );
    }
}
