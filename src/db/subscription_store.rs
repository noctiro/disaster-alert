use crate::models::{DestinationId, Subscription, mask_device_key};
use crate::utils::region;
use anyhow::{Result, anyhow};
use sled::Db;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone)]
pub struct SubscriptionStore {
    db: Db,
    cache: Arc<RwLock<SubscriptionCache>>,
    write_gate: Arc<Mutex<()>>,
}

#[derive(Clone)]
pub struct SubscriptionSnapshot {
    pub subscription: Arc<Subscription>,
    version: Arc<Subscription>,
}

impl SubscriptionSnapshot {
    pub(crate) fn new(subscription: Arc<Subscription>) -> Self {
        Self {
            version: Arc::clone(&subscription),
            subscription,
        }
    }
}

struct SubscriptionCache {
    by_destination: HashMap<DestinationId, Arc<Subscription>>,
    snapshot: Arc<Vec<Arc<Subscription>>>,
    spatial: Arc<SubscriptionIndex>,
    snapshot_dirty: bool,
}

#[derive(Default)]
struct SubscriptionIndex {
    grid: HashMap<(i16, i16), Vec<Arc<Subscription>>>,
    regions: HashMap<String, Vec<Arc<Subscription>>>,
}

pub enum SubscriptionCandidateQuery<'a> {
    All,
    Destinations(&'a HashSet<DestinationId>),
    Regions(&'a [String]),
    Radius {
        latitude: f64,
        longitude: f64,
        radius_km: f64,
    },
    RadiusOrRegions {
        latitude: f64,
        longitude: f64,
        radius_km: f64,
        regions: &'a [String],
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreErrorKind {
    NotFound,
    Internal,
}

#[derive(Debug)]
struct SubscriptionNotFound;

impl fmt::Display for SubscriptionNotFound {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("订阅不存在")
    }
}

impl std::error::Error for SubscriptionNotFound {}

impl SubscriptionStore {
    pub(crate) fn new(db: Db) -> Result<Self> {
        let mut subscriptions = HashMap::new();
        for item in db.scan_prefix(b"sub:") {
            let (key, value) = item?;
            match serde_json::from_slice::<Subscription>(&value) {
                Ok(subscription)
                    if subscription_key_matches(&key, &subscription.destination_id()) =>
                {
                    subscription.validate().map_err(|error| {
                        anyhow!(
                            "invalid subscription record {}: {error}",
                            mask_subscription_key(&key)
                        )
                    })?;
                    subscriptions.insert(subscription.destination_id(), Arc::new(subscription));
                }
                Ok(_subscription) => {
                    anyhow::bail!(
                        "subscription record key mismatch: {}",
                        mask_subscription_key(&key)
                    );
                }
                Err(error) => {
                    return Err(anyhow!(
                        "invalid subscription record {}: {error}",
                        mask_subscription_key(&key)
                    ));
                }
            }
        }
        let snapshot = Arc::new(
            subscriptions
                .values()
                .cloned()
                .collect::<Vec<Arc<Subscription>>>(),
        );
        let spatial = Arc::new(build_index(&snapshot));
        Ok(Self {
            db,
            cache: Arc::new(RwLock::new(SubscriptionCache {
                by_destination: subscriptions,
                snapshot,
                spatial,
                snapshot_dirty: false,
            })),
            write_gate: Arc::new(Mutex::new(())),
        })
    }

    pub(crate) fn flush(&self) -> Result<()> {
        let _write_guard = self.lock_write_gate();
        self.db.flush()?;
        Ok(())
    }

    pub fn upsert_subscription(&self, mut subscription: Subscription) -> Result<()> {
        subscription
            .validate()
            .map_err(|error| anyhow!("invalid subscription: {error}"))?;
        let destination_id = subscription.destination_id();
        let primary_key = subscription_key(&destination_id);
        let _write_guard = self.lock_write_gate();
        let existing_created_at = self
            .read_cache()
            .by_destination
            .get(&destination_id)
            .map(|existing| existing.created_at);
        subscription.prepare_for_upsert(existing_created_at);
        let primary_value = serde_json::to_vec(&subscription)?;
        let is_new_subscription = self
            .db
            .insert(primary_key.as_bytes(), primary_value)?
            .is_none();
        let mut cache = self.write_cache();
        cache
            .by_destination
            .insert(destination_id.clone(), Arc::new(subscription));
        cache.snapshot_dirty = true;

        tracing::info!(
            event = "subscription.stored",
            action = if is_new_subscription { "insert" } else { "update" },
            device_key = %mask_device_key(&destination_id.device_key),
            "subscription.stored"
        );

        Ok(())
    }

    pub fn delete_subscription(&self, destination_id: &DestinationId) -> Result<()> {
        let primary_key = subscription_key(destination_id);
        let _write_guard = self.lock_write_gate();
        if self.db.remove(primary_key.as_bytes())?.is_none() {
            return Err(SubscriptionNotFound.into());
        }
        let mut cache = self.write_cache();
        cache.by_destination.remove(destination_id);
        cache.snapshot_dirty = true;

        tracing::info!(
            event = "subscription.deleted",
            device_key = %mask_device_key(&destination_id.device_key),
            "subscription.deleted"
        );
        Ok(())
    }

    pub fn classify_error(error: &anyhow::Error) -> StoreErrorKind {
        if error.downcast_ref::<SubscriptionNotFound>().is_some() {
            StoreErrorKind::NotFound
        } else {
            StoreErrorKind::Internal
        }
    }

    pub fn for_each_subscription<F>(&self, mut visitor: F) -> Result<()>
    where
        F: FnMut(SubscriptionSnapshot) -> Result<()>,
    {
        let snapshot = self.current_snapshot();
        for subscription in snapshot.iter() {
            visitor(SubscriptionSnapshot::new(Arc::clone(subscription)))?;
        }
        Ok(())
    }

    pub fn for_each_in_radius<F>(
        &self,
        latitude: f64,
        longitude: f64,
        radius_km: f64,
        mut visitor: F,
    ) -> Result<()>
    where
        F: FnMut(SubscriptionSnapshot) -> Result<()>,
    {
        let index = self.current_index();
        let mut seen = HashSet::new();
        visit_radius_candidates(
            &index,
            latitude,
            longitude,
            radius_km,
            &mut seen,
            &mut visitor,
        )
    }

    pub fn for_each_in_regions<F>(&self, regions: &[String], mut visitor: F) -> Result<()>
    where
        F: FnMut(SubscriptionSnapshot) -> Result<()>,
    {
        let index = self.current_index();
        let mut seen = HashSet::new();
        visit_region_candidates(&index, regions, &mut seen, &mut visitor)
    }

    pub fn for_each_destination<F>(
        &self,
        destinations: &HashSet<DestinationId>,
        mut visitor: F,
    ) -> Result<()>
    where
        F: FnMut(SubscriptionSnapshot) -> Result<()>,
    {
        let subscriptions = {
            let cache = self.read_cache();
            destinations
                .iter()
                .filter_map(|destination| cache.by_destination.get(destination).cloned())
                .collect::<Vec<_>>()
        };
        for subscription in subscriptions {
            visitor(SubscriptionSnapshot::new(subscription))?;
        }
        Ok(())
    }

    pub fn for_each_candidate<F>(
        &self,
        query: SubscriptionCandidateQuery<'_>,
        mut visitor: F,
    ) -> Result<()>
    where
        F: FnMut(SubscriptionSnapshot) -> Result<()>,
    {
        match query {
            SubscriptionCandidateQuery::All => self.for_each_subscription(visitor),
            SubscriptionCandidateQuery::Destinations(destinations) => {
                self.for_each_destination(destinations, visitor)
            }
            SubscriptionCandidateQuery::Regions(regions) => {
                self.for_each_in_regions(regions, visitor)
            }
            SubscriptionCandidateQuery::Radius {
                latitude,
                longitude,
                radius_km,
            } => self.for_each_in_radius(latitude, longitude, radius_km, visitor),
            SubscriptionCandidateQuery::RadiusOrRegions {
                latitude,
                longitude,
                radius_km,
                regions,
            } => {
                let index = self.current_index();
                let mut seen = HashSet::new();
                visit_radius_candidates(
                    &index,
                    latitude,
                    longitude,
                    radius_km,
                    &mut seen,
                    &mut visitor,
                )?;
                visit_region_candidates(&index, regions, &mut seen, &mut visitor)
            }
        }
    }

    fn current_snapshot(&self) -> Arc<Vec<Arc<Subscription>>> {
        {
            let cache = self.read_cache();
            if !cache.snapshot_dirty {
                return cache.snapshot.clone();
            }
        }
        self.rebuild_indexes().0
    }

    fn current_index(&self) -> Arc<SubscriptionIndex> {
        {
            let cache = self.read_cache();
            if !cache.snapshot_dirty {
                return cache.spatial.clone();
            }
        }
        self.rebuild_indexes().1
    }

    fn rebuild_indexes(&self) -> (Arc<Vec<Arc<Subscription>>>, Arc<SubscriptionIndex>) {
        let mut cache = self.write_cache();
        if cache.snapshot_dirty {
            let snapshot = Arc::new(
                cache
                    .by_destination
                    .values()
                    .cloned()
                    .collect::<Vec<Arc<Subscription>>>(),
            );
            cache.spatial = Arc::new(build_index(&snapshot));
            cache.snapshot = snapshot;
            cache.snapshot_dirty = false;
        }
        (cache.snapshot.clone(), cache.spatial.clone())
    }

    pub fn get_total_count(&self) -> Result<usize> {
        Ok(self.read_cache().by_destination.len())
    }

    fn lock_write_gate(&self) -> MutexGuard<'_, ()> {
        match self.write_gate.lock() {
            Ok(guard) => guard,
            Err(error) => {
                tracing::error!(
                    event = "subscription.write_lock_recovered",
                    "subscription.write_lock_recovered"
                );
                error.into_inner()
            }
        }
    }

    fn read_cache(&self) -> RwLockReadGuard<'_, SubscriptionCache> {
        match self.cache.read() {
            Ok(guard) => guard,
            Err(error) => {
                tracing::error!(
                    event = "subscription.cache_lock_recovered",
                    "subscription.cache_lock_recovered"
                );
                error.into_inner()
            }
        }
    }

    fn write_cache(&self) -> RwLockWriteGuard<'_, SubscriptionCache> {
        match self.cache.write() {
            Ok(guard) => guard,
            Err(error) => {
                tracing::error!(
                    event = "subscription.cache_lock_recovered",
                    "subscription.cache_lock_recovered"
                );
                error.into_inner()
            }
        }
    }

    pub fn is_current(&self, snapshot: &SubscriptionSnapshot) -> bool {
        self.read_cache()
            .by_destination
            .get(&snapshot.subscription.destination_id())
            .is_some_and(|current| Arc::ptr_eq(current, &snapshot.version))
    }
}

fn build_index(subscriptions: &[Arc<Subscription>]) -> SubscriptionIndex {
    let mut index = SubscriptionIndex::default();
    for subscription in subscriptions {
        let mut cells = HashSet::new();
        let mut regions = HashSet::new();
        for target in &subscription.targets {
            cells.insert(grid_cell(target.point.latitude, target.point.longitude));
            for value in [
                &target.region.province,
                &target.region.city,
                &target.region.district,
            ] {
                let region = region::normalize(value);
                if !region.is_empty() {
                    regions.insert(region);
                }
            }
        }
        for cell in cells {
            index
                .grid
                .entry(cell)
                .or_default()
                .push(Arc::clone(subscription));
        }
        for region in regions {
            index
                .regions
                .entry(region)
                .or_default()
                .push(Arc::clone(subscription));
        }
    }
    index
}

fn visit_radius_candidates<F>(
    index: &SubscriptionIndex,
    latitude: f64,
    longitude: f64,
    radius_km: f64,
    seen: &mut HashSet<DestinationId>,
    visitor: &mut F,
) -> Result<()>
where
    F: FnMut(SubscriptionSnapshot) -> Result<()>,
{
    let Some(bounds) = radius_cell_bounds(latitude, longitude, radius_km) else {
        return Ok(());
    };
    for lat in bounds.min_lat..=bounds.max_lat {
        if bounds.lon_delta >= 180.0 {
            for lon in -180i16..=179 {
                visit_cell(index, (lat, lon), seen, visitor)?;
            }
        } else {
            let min_lon = (longitude - bounds.lon_delta).floor() as i32;
            let max_lon = (longitude + bounds.lon_delta).ceil() as i32;
            for lon in min_lon..=max_lon {
                let wrapped = ((lon + 180).rem_euclid(360) - 180) as i16;
                visit_cell(index, (lat, wrapped), seen, visitor)?;
            }
        }
    }
    Ok(())
}

fn visit_region_candidates<F>(
    index: &SubscriptionIndex,
    regions: &[String],
    seen: &mut HashSet<DestinationId>,
    visitor: &mut F,
) -> Result<()>
where
    F: FnMut(SubscriptionSnapshot) -> Result<()>,
{
    let regions = regions
        .iter()
        .map(|region| region::normalize(region))
        .filter(|region| !region.is_empty())
        .collect::<HashSet<_>>();
    for region in regions {
        let Some(subscriptions) = index.regions.get(&region) else {
            continue;
        };
        for subscription in subscriptions {
            if seen.insert(subscription.destination_id()) {
                visitor(SubscriptionSnapshot::new(Arc::clone(subscription)))?;
            }
        }
    }
    Ok(())
}

fn grid_cell(latitude: f64, longitude: f64) -> (i16, i16) {
    (
        latitude.floor().clamp(-90.0, 89.0) as i16,
        normalize_longitude(longitude).floor().clamp(-180.0, 179.0) as i16,
    )
}

struct RadiusCellBounds {
    min_lat: i16,
    max_lat: i16,
    lon_delta: f64,
}

fn radius_cell_bounds(latitude: f64, longitude: f64, radius_km: f64) -> Option<RadiusCellBounds> {
    if !(-90.0..=90.0).contains(&latitude)
        || !(-180.0..=180.0).contains(&longitude)
        || !radius_km.is_finite()
        || radius_km < 0.0
    {
        return None;
    }
    let lat_delta = radius_km / 110.0 + 1.0;
    let min_lat = (latitude - lat_delta).floor().clamp(-90.0, 89.0) as i16;
    let max_lat = (latitude + lat_delta).ceil().clamp(-90.0, 89.0) as i16;
    let edge_latitude = (latitude.abs() + lat_delta).min(89.9).to_radians();
    let lon_delta = (radius_km / (111.0 * edge_latitude.cos()).max(0.01) + 1.0).min(180.0);
    Some(RadiusCellBounds {
        min_lat,
        max_lat,
        lon_delta,
    })
}

fn visit_cell<F>(
    index: &SubscriptionIndex,
    cell: (i16, i16),
    seen: &mut HashSet<DestinationId>,
    visitor: &mut F,
) -> Result<()>
where
    F: FnMut(SubscriptionSnapshot) -> Result<()>,
{
    for subscription in index.grid.get(&cell).into_iter().flatten() {
        if seen.insert(subscription.destination_id()) {
            visitor(SubscriptionSnapshot::new(Arc::clone(subscription)))?;
        }
    }
    Ok(())
}

fn normalize_longitude(longitude: f64) -> f64 {
    (longitude + 180.0).rem_euclid(360.0) - 180.0
}

fn mask_subscription_key(key: &[u8]) -> String {
    String::from_utf8_lossy(key)
        .rsplit(':')
        .next()
        .map(mask_device_key)
        .unwrap_or_else(|| "***".to_string())
}

fn subscription_key(destination: &DestinationId) -> String {
    format!(
        "sub:{}:{}:{}",
        destination.base_url.len(),
        destination.base_url,
        destination.device_key
    )
}

fn subscription_key_matches(key: &[u8], destination: &DestinationId) -> bool {
    key == subscription_key(destination).as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        AdministrativeRegion, AlertRule, GeoPoint, MonitoringTarget, NotificationDestination,
    };
    use std::sync::{MutexGuard, OnceLock};

    fn database_test_guard() -> Result<MutexGuard<'static, ()>> {
        static DATABASE_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        Ok(
            match DATABASE_TEST_LOCK.get_or_init(|| Mutex::new(())).lock() {
                Ok(guard) => guard,
                Err(error) => error.into_inner(),
            },
        )
    }

    fn temporary_store() -> Result<SubscriptionStore> {
        let db = sled::Config::new().temporary(true).open()?;
        SubscriptionStore::new(db)
    }

    fn subscription(device_key: &str, lat: f64, lon: f64) -> Subscription {
        Subscription::new(
            NotificationDestination::Bark {
                base_url: "https://api.day.app".to_string(),
                device_key: device_key.to_string(),
            },
            vec![MonitoringTarget {
                label: "home".to_string(),
                point: GeoPoint {
                    latitude: lat,
                    longitude: lon,
                },
                region: AdministrativeRegion::default(),
            }],
            crate::models::DisasterCategory::ALL
                .into_iter()
                .map(AlertRule::default_for)
                .collect(),
        )
    }

    fn destination(device_key: &str) -> DestinationId {
        DestinationId {
            base_url: "https://api.day.app".to_string(),
            device_key: device_key.to_string(),
        }
    }

    fn collect_subscriptions(store: &SubscriptionStore) -> Result<Vec<Subscription>> {
        let mut subscriptions = Vec::new();
        store.for_each_subscription(|snapshot| {
            subscriptions.push((*snapshot.subscription).clone());
            Ok(())
        })?;
        Ok(subscriptions)
    }

    #[test]
    fn primary_records_are_globally_iterable_and_track_updates() -> Result<()> {
        let _database_guard = database_test_guard()?;
        let store = temporary_store()?;
        let beijing = subscription("abc123", 39.9042, 116.4074);
        let shanghai = subscription("abc123", 31.2397, 121.4999);

        store.upsert_subscription(beijing)?;
        let found = collect_subscriptions(&store)?;
        anyhow::ensure!(found.len() == 1, "expected one beijing subscription");
        anyhow::ensure!(found[0].device_key() == "abc123", "unexpected device key");

        store.upsert_subscription(shanghai)?;
        let updated = collect_subscriptions(&store)?;
        anyhow::ensure!(updated.len() == 1, "expected one updated subscription");
        anyhow::ensure!(
            updated[0].targets[0].point.longitude == 121.4999,
            "unexpected longitude"
        );

        store.upsert_subscription(subscription("tokyo1", 35.6762, 139.6503))?;
        store.upsert_subscription(subscription("london1", 51.5072, -0.1276))?;
        let subscriptions = collect_subscriptions(&store)?;
        anyhow::ensure!(
            subscriptions.len() == 3,
            "all subscriptions must be evaluated globally"
        );

        store.delete_subscription(&destination("abc123"))?;
        let after_delete = collect_subscriptions(&store)?;
        anyhow::ensure!(
            after_delete.len() == 2,
            "deleted subscription must not be returned"
        );

        Ok(())
    }

    #[test]
    fn snapshot_scan_scales_without_database_reads() -> Result<()> {
        let _database_guard = database_test_guard()?;
        let store = temporary_store()?;
        {
            let mut cache = store.write_cache();
            for index in 0..100_000 {
                let subscription = subscription(&format!("device{index:06}"), 35.6762, 139.6503);
                cache
                    .by_destination
                    .insert(subscription.destination_id(), Arc::new(subscription));
            }
            cache.snapshot_dirty = true;
        }

        let mut count = 0usize;
        store.for_each_subscription(|_snapshot| {
            count += 1;
            Ok(())
        })?;
        anyhow::ensure!(
            count == 100_000,
            "snapshot scan must include every subscription"
        );

        Ok(())
    }

    #[test]
    fn display_names_do_not_create_administrative_region_matches() -> Result<()> {
        let mut named = subscription("device1", 30.0, 120.0);
        named.targets[0].label = "北京".to_string();
        let index = build_index(&[Arc::new(named)]);
        let mut count = 0;
        visit_region_candidates(
            &index,
            &["北京市".to_string()],
            &mut HashSet::new(),
            &mut |_snapshot| {
                count += 1;
                Ok(())
            },
        )?;
        anyhow::ensure!(count == 0, "display labels must not be indexed as regions");
        Ok(())
    }

    #[test]
    fn concurrent_writes_keep_persistence_and_snapshot_consistent() -> Result<()> {
        let _database_guard = database_test_guard()?;
        let db = sled::Config::new().temporary(true).open()?;
        let store = SubscriptionStore::new(db.clone())?;
        let mut writers = Vec::new();
        for index in 0..32 {
            let store = store.clone();
            writers.push(std::thread::spawn(move || {
                store.upsert_subscription(subscription(
                    &format!("device{index:02}"),
                    35.6762,
                    139.6503,
                ))
            }));
        }
        for writer in writers {
            match writer.join() {
                Ok(result) => result?,
                Err(_panic_payload) => return Err(anyhow!("subscription writer panicked")),
            }
        }

        anyhow::ensure!(
            store.get_total_count()? == 32,
            "snapshot must include all writes"
        );
        let reloaded = SubscriptionStore::new(db)?;
        anyhow::ensure!(
            reloaded.get_total_count()? == 32,
            "persisted records must match the snapshot"
        );

        Ok(())
    }

    #[test]
    fn stale_snapshot_is_invalidated_by_an_update_or_delete() -> Result<()> {
        let _database_guard = database_test_guard()?;
        let store = temporary_store()?;
        store.upsert_subscription(subscription("device1", 35.6762, 139.6503))?;

        let mut snapshot = None;
        store.for_each_subscription(|current| {
            snapshot = Some(current);
            Ok(())
        })?;
        let Some(snapshot) = snapshot else {
            anyhow::bail!("expected a subscription snapshot");
        };
        anyhow::ensure!(store.is_current(&snapshot), "snapshot should start current");

        store.upsert_subscription(subscription("device1", 51.5072, -0.1276))?;
        anyhow::ensure!(
            !store.is_current(&snapshot),
            "update must invalidate old snapshot"
        );

        let mut replacement = None;
        store.for_each_subscription(|current| {
            replacement = Some(current);
            Ok(())
        })?;
        let Some(replacement) = replacement else {
            anyhow::bail!("expected replacement snapshot");
        };
        anyhow::ensure!(
            store.is_current(&replacement),
            "replacement must be current"
        );

        store.delete_subscription(&destination("device1"))?;
        anyhow::ensure!(
            !store.is_current(&replacement),
            "delete must invalidate snapshot"
        );

        Ok(())
    }

    #[test]
    fn same_device_key_on_different_bark_servers_is_independent() -> Result<()> {
        let _database_guard = database_test_guard()?;
        let store = temporary_store()?;
        let first = subscription("shared", 35.0, 105.0);
        let mut second = subscription("shared", 36.0, 106.0);
        second.destination = NotificationDestination::Bark {
            base_url: "https://bark.example.com".to_string(),
            device_key: "shared".to_string(),
        };
        let second_id = second.destination_id();

        store.upsert_subscription(first)?;
        store.upsert_subscription(second)?;
        anyhow::ensure!(
            store.get_total_count()? == 2,
            "destinations must not collide"
        );
        store.delete_subscription(&second_id)?;
        anyhow::ensure!(
            store.get_total_count()? == 1,
            "delete must be destination-scoped"
        );
        Ok(())
    }

    #[test]
    fn missing_subscription_uses_a_typed_not_found_error() -> Result<()> {
        let _database_guard = database_test_guard()?;
        let store = temporary_store()?;
        let error = match store.delete_subscription(&destination("missing")) {
            Ok(()) => anyhow::bail!("expected deletion to fail"),
            Err(error) => error,
        };

        anyhow::ensure!(
            SubscriptionStore::classify_error(&error) == StoreErrorKind::NotFound,
            "missing subscriptions must map to 404 independently of error text"
        );
        Ok(())
    }
}
