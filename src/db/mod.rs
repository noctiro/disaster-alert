use anyhow::{Context, Result};
use std::path::Path;

mod subscription_store;

pub use subscription_store::{
    StoreErrorKind, SubscriptionCandidateQuery, SubscriptionSnapshot, SubscriptionStore,
};

/// 数据库封装
#[derive(Clone)]
pub struct Database {
    subscriptions: SubscriptionStore,
}

impl Database {
    /// 打开数据库
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let db = sled::open(path)
            .with_context(|| format!("failed to open database at {}", path.display()))?;
        Ok(Self {
            subscriptions: SubscriptionStore::new(db)?,
        })
    }

    /// 获取订阅存储
    pub fn subscriptions(&self) -> SubscriptionStore {
        self.subscriptions.clone()
    }

    /// Persist all pending database writes before process shutdown.
    pub async fn flush(&self) -> Result<()> {
        let subscriptions = self.subscriptions.clone();
        tokio::task::spawn_blocking(move || subscriptions.flush())
            .await
            .context("database flush task failed")??;
        Ok(())
    }
}
