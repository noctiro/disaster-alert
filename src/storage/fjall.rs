use super::{decode_record, encode_record};
use crate::delivery::{DeadLetterItem, DeliveryBatch, DeliverySuccess, RetryItem};
use crate::events::MatchJob;
use crate::matching::{MatchPlan, MatchScope, PostingBlock};
use crate::models::{
    DisasterCategory, DisasterEvent, IncidentCapacity, IncidentId, IncidentRecord, ProviderChannel,
    Subscription, parse_event_epoch,
};
use crate::subscriptions::{
    CompiledSubscription, DestinationNumericId, MatchPostingKey, SubscriptionCompiler,
    SubscriptionId,
};
use anyhow::{Context, Result};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use roaring::RoaringBitmap;
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

const FORMAT_VERSION: &[u8] = b"1";
const MAX_RECORD_BYTES: usize = 512 * 1024;
const CORRELATION_WINDOW_SECONDS: i64 = 120;
const CORRELATION_DISTANCE_KM: f64 = 100.0;
const CORRELATION_MAGNITUDE_DELTA: f64 = 1.0;
const MAX_CORRELATION_CANDIDATES: usize = 1_024;

#[derive(Clone)]
pub(crate) struct FjallStorage {
    db: Database,
    id_lock: Arc<Mutex<()>>,
    subscription_lock: Arc<Mutex<()>>,
    match_lock: Arc<Mutex<()>>,
    retry_lock: Arc<Mutex<()>>,
    inbox: Keyspace,
    rejected_inbox: Keyspace,
    incidents: Keyspace,
    incident_aliases: Keyspace,
    incident_aliases_by_incident: Keyspace,
    incident_correlation: Keyspace,
    incident_correlation_by_incident: Keyspace,
    events: Keyspace,
    match_jobs: Keyspace,
    subscriptions: Keyspace,
    subscriptions_by_destination: Keyspace,
    compiled_subscriptions: Keyspace,
    postings: Keyspace,
    delivery_batches: Keyspace,
    delivery_progress: Keyspace,
    delivery_by_destination: Keyspace,
    retries: Keyspace,
    retries_by_destination: Keyspace,
    retries_by_batch: Keyspace,
    dead_letters: Keyspace,
    ledger: Keyspace,
    contexts: Keyspace,
    meta: Keyspace,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredSubscription {
    pub(crate) id: SubscriptionId,
    pub(crate) destination_id: DestinationNumericId,
    pub(crate) generation: u64,
    pub(crate) active: bool,
    pub(crate) subscription: Subscription,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InboxItem {
    pub(crate) id: u64,
    pub(crate) provider: ProviderChannel,
    pub(crate) received_at_ms: i64,
    pub(crate) event: DisasterEvent,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RejectedInboxItem {
    item: InboxItem,
    rejected_at_ms: i64,
    reason: String,
}

#[derive(Debug)]
pub(crate) struct IncidentResolutionCapacity(pub(crate) IncidentCapacity);

impl std::fmt::Display for IncidentResolutionCapacity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "incident capacity exceeded: {:?}", self.0)
    }
}

impl std::error::Error for IncidentResolutionCapacity {}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredDelivery {
    delivered_at_ms: i64,
    event_revision: u64,
    row: crate::delivery::DeliveryRow,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StoragePruneStats {
    pub(crate) incidents: usize,
    pub(crate) delivery_records: usize,
    pub(crate) events: usize,
}

impl FjallStorage {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::builder(path)
            .open()
            .context("failed to open Fjall database")?;
        let keyspace = |name| {
            db.keyspace(name, KeyspaceCreateOptions::default)
                .with_context(|| format!("failed to open Fjall keyspace {name}"))
        };
        let storage = Self {
            id_lock: Arc::new(Mutex::new(())),
            subscription_lock: Arc::new(Mutex::new(())),
            match_lock: Arc::new(Mutex::new(())),
            retry_lock: Arc::new(Mutex::new(())),
            inbox: keyspace("inbox")?,
            rejected_inbox: keyspace("rejected_inbox")?,
            incidents: keyspace("incidents")?,
            incident_aliases: keyspace("incident_aliases")?,
            incident_aliases_by_incident: keyspace("incident_aliases_by_incident")?,
            incident_correlation: keyspace("incident_correlation")?,
            incident_correlation_by_incident: keyspace("incident_correlation_by_incident")?,
            events: keyspace("events")?,
            match_jobs: keyspace("match_jobs")?,
            subscriptions: keyspace("subscriptions")?,
            subscriptions_by_destination: keyspace("subscriptions_by_destination")?,
            compiled_subscriptions: keyspace("compiled_subscriptions")?,
            postings: keyspace("postings")?,
            delivery_batches: keyspace("delivery_batches")?,
            delivery_progress: keyspace("delivery_progress")?,
            delivery_by_destination: keyspace("delivery_by_destination")?,
            retries: keyspace("retries")?,
            retries_by_destination: keyspace("retries_by_destination")?,
            retries_by_batch: keyspace("retries_by_batch")?,
            dead_letters: keyspace("dead_letters")?,
            ledger: keyspace("ledger")?,
            contexts: keyspace("contexts")?,
            meta: keyspace("meta")?,
            db,
        };
        storage.initialize()?;
        Ok(storage)
    }

    fn initialize(&self) -> Result<()> {
        match self.meta.get(b"format_version")? {
            Some(value) => anyhow::ensure!(
                value.as_ref() == FORMAT_VERSION,
                "unsupported Fjall database format"
            ),
            None => self.meta.insert(b"format_version", FORMAT_VERSION)?,
        }
        Ok(())
    }

    pub(crate) fn persist(&self) -> Result<()> {
        self.db
            .persist(PersistMode::SyncAll)
            .context("failed to persist Fjall journal")
    }

    pub(crate) fn lock_incident_pipeline(&self) -> Result<MutexGuard<'_, ()>> {
        self.match_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall matching lock poisoned: {error}"))
    }

    #[cfg(feature = "migration")]
    pub(crate) fn bind_migration_source(
        &self,
        source_fingerprint: [u8; 32],
        existing_partial: bool,
    ) -> Result<()> {
        let key = b"migration:source";
        let existing = self.meta.get(key)?;
        if let Some(existing) = existing {
            anyhow::ensure!(
                existing.as_ref() == source_fingerprint,
                "Fjall target is bound to a different source"
            );
        } else {
            anyhow::ensure!(
                self.active_subscription_count()? == 0,
                if existing_partial {
                    "unbound partial Fjall target contains subscriptions"
                } else {
                    "migration target contains subscriptions"
                }
            );
            anyhow::ensure!(
                self.pending_inbox(1)?.is_empty()
                    && self.pending_match_jobs(1)?.is_empty()
                    && self.pending_delivery_batches(1)?.is_empty(),
                "migration target contains runtime work"
            );
            self.meta.insert(key, source_fingerprint)?;
        }
        Ok(())
    }

    #[cfg(feature = "migration")]
    pub(crate) fn verify_migration_source(&self, source_fingerprint: [u8; 32]) -> Result<()> {
        anyhow::ensure!(
            self.meta
                .get(b"migration:source")?
                .is_some_and(|value| value.as_ref() == source_fingerprint),
            "Fjall target is not bound to this source"
        );
        anyhow::ensure!(
            self.pending_inbox(1)?.is_empty()
                && self.pending_match_jobs(1)?.is_empty()
                && self.pending_delivery_batches(1)?.is_empty(),
            "migration target contains runtime work"
        );
        Ok(())
    }

    pub(crate) fn next_id(&self, name: &str) -> Result<u64> {
        let _lock = self
            .id_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall ID lock poisoned: {error}"))?;
        let mut key = b"next:".to_vec();
        key.extend_from_slice(name.as_bytes());
        let current = self
            .meta
            .get(&key)?
            .map(|value| decode_u64(&value))
            .transpose()?
            .unwrap_or(0);
        let next = current.checked_add(1).context("Fjall ID space exhausted")?;
        self.meta.insert(key, next.to_be_bytes())?;
        Ok(next)
    }

    pub(crate) fn ingest_with_cursor(
        &self,
        provider: ProviderChannel,
        events: Vec<DisasterEvent>,
        cursor: Option<(&str, &str)>,
    ) -> Result<Vec<u64>> {
        anyhow::ensure!(events.len() <= 1_024, "Inbox batch exceeds 1024 events");
        let received_at_ms = super::try_now_millis()?;
        if let Some((stream, _)) = cursor {
            anyhow::ensure!(
                crate::source_registry::find_provider(provider, stream).is_some(),
                "provider cursor references an unknown source"
            );
        }
        let mut batch = self.db.batch();
        let mut ids = Vec::with_capacity(events.len());
        for event in events {
            let source = crate::source_registry::find(&event.source);
            anyhow::ensure!(
                event.channel == provider
                    && source.is_some_and(|source| {
                        source.channel == provider && source.category == event.category
                    }),
                "Inbox event source, category, and provider do not agree"
            );
            let id = self.next_id("inbox")?;
            let item = InboxItem {
                id,
                provider,
                received_at_ms,
                event,
            };
            batch.insert(&self.inbox, id.to_be_bytes(), encode(&item)?);
            ids.push(id);
        }
        if let Some((stream, value)) = cursor {
            batch.insert(&self.meta, cursor_key(provider, stream), value.as_bytes());
        }
        batch
            .commit()
            .context("failed to atomically commit Inbox and cursor")?;
        Ok(ids)
    }

    pub(crate) fn provider_cursors(
        &self,
        provider: ProviderChannel,
        streams: &[String],
    ) -> Result<Vec<(String, String)>> {
        let mut cursors = Vec::new();
        for stream in streams {
            if let Some(value) = self.meta.get(cursor_key(provider, stream))? {
                cursors.push((
                    stream.clone(),
                    std::str::from_utf8(&value)
                        .context("provider cursor is not UTF-8")?
                        .to_string(),
                ));
            }
        }
        Ok(cursors)
    }

    pub(crate) fn pending_inbox(&self, limit: usize) -> Result<Vec<InboxItem>> {
        self.inbox
            .iter()
            .take(limit)
            .map(|item| decode(&item.value()?))
            .collect()
    }

    pub(crate) fn backlog_counts(&self) -> Result<super::BacklogCounts> {
        let mut delivery_batches = 0usize;
        for item in self.delivery_batches.prefix([0]) {
            drop(item.key()?);
            delivery_batches = delivery_batches.saturating_add(1);
        }
        Ok(super::BacklogCounts {
            inbox: self.inbox.len()?,
            match_jobs: self.match_jobs.len()?,
            delivery_batches,
            retries: self.retries.len()?,
        })
    }

    pub(crate) fn event(&self, revision: u64) -> Result<Option<DisasterEvent>> {
        get_record(&self.events, &revision.to_be_bytes())
    }

    pub(crate) fn incident(&self, id: &IncidentId) -> Result<Option<IncidentRecord>> {
        get_record(&self.incidents, id.as_str().as_bytes())
    }

    pub(crate) fn resolve_incident(&self, event: &DisasterEvent) -> Result<IncidentId> {
        let event_key = event.event_key();
        let alias = incident_alias(&event_key);
        if let Some(value) = self.incident_aliases.get(alias)? {
            let id = std::str::from_utf8(&value).context("incident alias ID is not UTF-8")?;
            let id = IncidentId::parse(id).context("incident alias contains an invalid ID")?;
            anyhow::ensure!(
                self.incident(&id)?.is_some(),
                "incident alias references a missing record"
            );
            return Ok(id);
        }
        if let Some(id) = self.find_correlated_incident(event)? {
            return Ok(id);
        }
        Ok(IncidentId::derive(&event_key))
    }

    fn find_correlated_incident(&self, event: &DisasterEvent) -> Result<Option<IncidentId>> {
        if !is_earthquake(event.category) {
            return Ok(None);
        }
        let Some(occurred_epoch) = parse_event_epoch(event) else {
            return Ok(None);
        };
        let start = correlation_key(
            occurred_epoch.saturating_sub(CORRELATION_WINDOW_SECONDS),
            "",
            &[0; 32],
        );
        let end = correlation_key(
            occurred_epoch.saturating_add(CORRELATION_WINDOW_SECONDS),
            &"~".repeat(22),
            &[u8::MAX; 32],
        );
        let mut best: Option<(IncidentId, f64)> = None;
        for (candidates, item) in self
            .incident_correlation
            .range(start.as_slice()..=end.as_slice())
            .enumerate()
        {
            if candidates >= MAX_CORRELATION_CANDIDATES {
                return Err(
                    IncidentResolutionCapacity(IncidentCapacity::CorrelationCandidates).into(),
                );
            }
            let (key, value) = item.into_inner()?;
            let candidate_epoch = correlation_epoch(&key)?;
            let id = std::str::from_utf8(&value).context("correlation ID is not UTF-8")?;
            let id = IncidentId::parse(id).context("correlation contains an invalid ID")?;
            let record = self
                .incident(&id)?
                .context("correlation references a missing incident")?;
            if record
                .latest_by_source
                .iter()
                .any(|current| current.source == event.source)
            {
                continue;
            }
            for current in &record.latest_by_source {
                let Some(score) = correlation_score(
                    current,
                    event,
                    candidate_epoch.abs_diff(occurred_epoch) as f64,
                ) else {
                    continue;
                };
                if best
                    .as_ref()
                    .is_none_or(|(_, current_score)| score < *current_score)
                {
                    best = Some((id.clone(), score));
                }
            }
        }
        Ok(best.map(|(id, _)| id))
    }

    pub(crate) fn commit_incident_match_job(
        &self,
        incident: &IncidentRecord,
        event: &DisasterEvent,
        job: &MatchJob,
        inbox_id: u64,
    ) -> Result<()> {
        self.commit_incident(incident, event, Some(job), inbox_id)
    }

    fn commit_incident(
        &self,
        incident: &IncidentRecord,
        event: &DisasterEvent,
        job: Option<&MatchJob>,
        inbox_id: u64,
    ) -> Result<()> {
        let existing = self.incident(&incident.id)?;
        let mut incident = incident.clone();
        if let Some(current) = &existing {
            incident.has_matched_subscribers |= current.has_matched_subscribers;
            incident.pending_match_jobs = current.pending_match_jobs;
        }
        if job.is_some() {
            incident.pending_match_jobs = incident
                .pending_match_jobs
                .checked_add(1)
                .context("Incident pending MatchJob count overflowed")?;
        }
        if job.is_none()
            && !incident.has_matched_subscribers
            && incident.pending_match_jobs == 0
            && self.ledger.prefix(incident.id.as_str()).next().is_none()
        {
            let mut batch = self.db.batch();
            if let Some(existing) = &existing {
                self.remove_incident_indexes(&mut batch, existing)?;
                batch.remove(&self.incidents, existing.id.as_str());
            }
            batch.remove(&self.inbox, inbox_id.to_be_bytes());
            return batch
                .commit()
                .context("failed to discard unmatched Incident transition");
        }
        for event_key in &incident.source_event_keys {
            let alias = incident_alias(event_key);
            if let Some(existing) = self.incident_aliases.get(alias)? {
                anyhow::ensure!(
                    existing.as_ref() == incident.id.as_str().as_bytes(),
                    "incident alias already belongs to another incident"
                );
            }
        }
        let mut batch = self.db.batch();
        batch.insert(&self.incidents, incident.id.as_str(), encode(&incident)?);
        if let Some(job) = job {
            batch.insert(
                &self.events,
                job.event_revision.to_be_bytes(),
                encode(event)?,
            );
            batch.insert(&self.match_jobs, job.id.to_be_bytes(), encode(job)?);
        }
        for event_key in &incident.source_event_keys {
            let alias = incident_alias(event_key);
            batch.insert(&self.incident_aliases, alias, incident.id.as_str());
            batch.insert(
                &self.incident_aliases_by_incident,
                incident_alias_reverse_key(&incident.id, &alias),
                alias,
            );
        }
        if is_earthquake(event.category)
            && let Some(occurred_epoch) = parse_event_epoch(event)
        {
            let stream = correlation_stream(event);
            let reverse_key = correlation_reverse_key(&incident.id, &stream);
            if let Some(previous) = self.incident_correlation_by_incident.get(&reverse_key)? {
                batch.remove(&self.incident_correlation, previous);
            }
            let key = correlation_key(occurred_epoch, incident.id.as_str(), &stream);
            batch.insert(&self.incident_correlation, &key, incident.id.as_str());
            batch.insert(&self.incident_correlation_by_incident, reverse_key, key);
        }
        batch.remove(&self.inbox, inbox_id.to_be_bytes());
        batch
            .commit()
            .context("failed to commit Incident transition")
    }

    pub(crate) fn commit_incident_without_match(
        &self,
        incident: &IncidentRecord,
        event: &DisasterEvent,
        inbox_id: u64,
    ) -> Result<()> {
        self.commit_incident(incident, event, None, inbox_id)
    }

    pub(crate) fn complete_inbox(&self, inbox_id: u64) -> Result<()> {
        self.inbox.remove(inbox_id.to_be_bytes())?;
        Ok(())
    }

    pub(crate) fn reject_inbox(&self, item: InboxItem, reason: String) -> Result<()> {
        anyhow::ensure!(reason.len() <= 1_024, "Inbox rejection reason is oversized");
        let record = RejectedInboxItem {
            item,
            rejected_at_ms: super::try_now_millis()?,
            reason,
        };
        let mut batch = self.db.batch();
        batch.insert(
            &self.rejected_inbox,
            record.item.id.to_be_bytes(),
            encode(&record)?,
        );
        batch.remove(&self.inbox, record.item.id.to_be_bytes());
        batch.commit().context("failed to quarantine Inbox item")
    }

    #[cfg(test)]
    pub(crate) fn rejected_inbox_count(&self) -> Result<usize> {
        self.rejected_inbox.len().map_err(Into::into)
    }

    pub(crate) fn pending_match_jobs(&self, limit: usize) -> Result<Vec<MatchJob>> {
        self.match_jobs
            .iter()
            .take(limit)
            .map(|item| {
                let value = item.value()?;
                decode(&value)
            })
            .collect()
    }

    pub(crate) fn next_match_job(&self) -> Result<Option<MatchJob>> {
        self.pending_match_jobs(1)
            .map(|jobs| jobs.into_iter().next())
    }

    pub(crate) fn match_job(&self, id: u64) -> Result<Option<MatchJob>> {
        get_record(&self.match_jobs, &id.to_be_bytes())
    }

    #[cfg(any(test, feature = "benchmarks"))]
    pub(crate) fn store_subscription(
        &self,
        mut subscription: Subscription,
    ) -> Result<StoredSubscription> {
        let _lock = self
            .subscription_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall mutation lock poisoned: {error}"))?;
        self.store_subscription_inner(&mut subscription, None)
    }

    pub(crate) fn activate_confirmation(
        &self,
        confirmation_id: u64,
        expected_confirmation: &[u8],
        mut subscription: Subscription,
    ) -> Result<bool> {
        let _lock = self
            .subscription_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall mutation lock poisoned: {error}"))?;
        let key = confirmation_key(confirmation_id);
        let Some(current) = self.meta.get(key)? else {
            return Ok(false);
        };
        if current.as_ref() != expected_confirmation {
            return Ok(false);
        }
        let destination_index = confirmation_destination_key(&subscription.destination_id());
        let expected_id = confirmation_id.to_be_bytes();
        if !self
            .meta
            .get(&destination_index)?
            .is_some_and(|value| value.as_ref() == expected_id)
        {
            return Ok(false);
        }
        self.store_subscription_inner(&mut subscription, Some(confirmation_id))?;
        Ok(true)
    }

    fn store_subscription_inner(
        &self,
        subscription: &mut Subscription,
        remove_confirmation_id: Option<u64>,
    ) -> Result<StoredSubscription> {
        subscription
            .validate()
            .map_err(|error| anyhow::anyhow!("invalid subscription: {error}"))?;
        let destination_key = destination_key(subscription);
        let existing_id = self
            .subscriptions_by_destination
            .get(destination_key)?
            .map(|value| decode_u64(&value))
            .transpose()?;
        let previous = existing_id
            .map(|id| get_record::<StoredSubscription>(&self.subscriptions, &id.to_be_bytes()))
            .transpose()?
            .flatten();
        let id = previous
            .as_ref()
            .map_or_else(|| self.next_id("subscription"), |value| Ok(value.id.0))?;
        let destination_id = previous.as_ref().map_or_else(
            || self.next_id("destination").map(DestinationNumericId),
            |value| Ok(value.destination_id),
        )?;
        let generation = previous
            .as_ref()
            .map_or(1, |value| value.generation.saturating_add(1));
        subscription
            .prepare_for_upsert(previous.as_ref().map(|value| value.subscription.created_at));
        let record = StoredSubscription {
            id: SubscriptionId(id),
            destination_id,
            generation,
            active: true,
            subscription: subscription.clone(),
        };
        let compiled = SubscriptionCompiler::compile(
            record.id,
            record.destination_id,
            record.generation,
            &record.subscription,
        )?;
        let previous_compiled = previous
            .as_ref()
            .map(|value| self.compiled_subscription(value.id))
            .transpose()?
            .flatten();
        self.commit_subscription_change(
            &record,
            &compiled,
            previous_compiled.as_ref(),
            remove_confirmation_id,
        )?;
        Ok(record)
    }

    pub(crate) fn deactivate_subscription(&self, subscription_id: SubscriptionId) -> Result<bool> {
        let _lock = self
            .subscription_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall mutation lock poisoned: {error}"))?;
        let Some(mut record) = get_record::<StoredSubscription>(
            &self.subscriptions,
            &subscription_id.0.to_be_bytes(),
        )?
        else {
            return Ok(false);
        };
        let old = self.compiled_subscription(subscription_id)?;
        record.active = false;
        record.generation = record.generation.saturating_add(1);
        let mut batch = self.db.batch();
        if let Some(old) = old.as_ref() {
            remove_postings(&self.postings, &mut batch, old)?;
        }
        batch.insert(
            &self.subscriptions,
            record.id.0.to_be_bytes(),
            encode(&record)?,
        );
        batch.remove(&self.compiled_subscriptions, record.id.0.to_be_bytes());
        let confirmation_destination =
            confirmation_destination_key(&record.subscription.destination_id());
        if let Some(id) = self
            .meta
            .get(&confirmation_destination)?
            .map(|value| decode_u64(&value))
            .transpose()?
        {
            batch.remove(&self.meta, confirmation_key(id));
            batch.remove(&self.meta, confirmation_destination);
        }
        batch.commit()?;
        Ok(true)
    }

    fn commit_subscription_change(
        &self,
        record: &StoredSubscription,
        compiled: &CompiledSubscription,
        previous: Option<&CompiledSubscription>,
        remove_confirmation_id: Option<u64>,
    ) -> Result<()> {
        let mut batch = self.db.batch();
        if let Some(previous) = previous {
            remove_postings(&self.postings, &mut batch, previous)?;
        }
        insert_postings(&self.postings, &mut batch, compiled)?;
        batch.insert(
            &self.subscriptions,
            record.id.0.to_be_bytes(),
            encode(record)?,
        );
        batch.insert(
            &self.subscriptions_by_destination,
            destination_key(&record.subscription),
            record.id.0.to_be_bytes(),
        );
        batch.insert(
            &self.compiled_subscriptions,
            record.id.0.to_be_bytes(),
            encode(compiled)?,
        );
        if let Some(id) = remove_confirmation_id {
            batch.remove(&self.meta, confirmation_key(id));
            batch.remove(
                &self.meta,
                confirmation_destination_key(&record.subscription.destination_id()),
            );
        }
        batch
            .commit()
            .context("failed to commit compiled subscription")
    }

    pub(crate) fn compiled_subscription(
        &self,
        id: SubscriptionId,
    ) -> Result<Option<CompiledSubscription>> {
        get_record(&self.compiled_subscriptions, &id.0.to_be_bytes())
    }

    pub(crate) fn stored_subscription(
        &self,
        id: SubscriptionId,
    ) -> Result<Option<StoredSubscription>> {
        get_record(&self.subscriptions, &id.0.to_be_bytes())
    }

    pub(crate) fn stored_subscription_by_destination(
        &self,
        destination: &crate::models::DestinationId,
    ) -> Result<Option<StoredSubscription>> {
        let key = destination_digest(destination);
        let Some(id) = self
            .subscriptions_by_destination
            .get(key)?
            .map(|value| decode_u64(&value))
            .transpose()?
        else {
            return Ok(None);
        };
        self.stored_subscription(SubscriptionId(id))
    }

    pub(crate) fn active_subscription_count(&self) -> Result<usize> {
        let mut count = 0usize;
        for item in self.subscriptions.iter() {
            let record: StoredSubscription = decode(&item.value()?)?;
            if record.active {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    #[cfg(any(feature = "migration", feature = "benchmarks"))]
    pub(crate) fn import_subscription_batch(
        &self,
        subscriptions: Vec<Subscription>,
    ) -> Result<usize> {
        anyhow::ensure!(
            !subscriptions.is_empty() && subscriptions.len() <= 5_000,
            "migration batch must contain 1..=5000 subscriptions"
        );
        let _lock = self
            .subscription_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall mutation lock poisoned: {error}"))?;
        let mut destinations = std::collections::HashSet::new();
        let mut prepared = Vec::with_capacity(subscriptions.len());
        for subscription in subscriptions {
            subscription
                .validate()
                .map_err(|error| anyhow::anyhow!("invalid migrated subscription: {error}"))?;
            let destination_key = destination_key(&subscription);
            anyhow::ensure!(
                destinations.insert(destination_key),
                "migration batch contains duplicate destinations"
            );
            anyhow::ensure!(
                self.subscriptions_by_destination
                    .get(destination_key)?
                    .is_none(),
                "migration target already contains a source destination"
            );
            let id = SubscriptionId(self.next_id("subscription")?);
            let destination_id = DestinationNumericId(self.next_id("destination")?);
            let record = StoredSubscription {
                id,
                destination_id,
                generation: 1,
                active: true,
                subscription,
            };
            let compiled = SubscriptionCompiler::compile(
                id,
                destination_id,
                record.generation,
                &record.subscription,
            )?;
            prepared.push((destination_key, record, compiled));
        }

        let mut posting_updates = std::collections::BTreeMap::<[u8; 20], RoaringBitmap>::new();
        for (_, _, compiled) in &prepared {
            for key in MatchPostingKey::for_subscription(compiled) {
                let encoded_key = key.encode();
                let bitmap = match posting_updates.entry(encoded_key) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let existing = self
                            .postings
                            .get(encoded_key)?
                            .map(|value| decode_bitmap(&value))
                            .transpose()?
                            .unwrap_or_default();
                        entry.insert(existing)
                    }
                };
                bitmap.insert(compiled.subscription_id.posting_offset());
            }
        }

        let count = prepared.len();
        let mut batch = self.db.batch();
        for (destination_key, record, compiled) in prepared {
            batch.insert(
                &self.subscriptions,
                record.id.0.to_be_bytes(),
                encode(&record)?,
            );
            batch.insert(
                &self.subscriptions_by_destination,
                destination_key,
                record.id.0.to_be_bytes(),
            );
            batch.insert(
                &self.compiled_subscriptions,
                record.id.0.to_be_bytes(),
                encode(&compiled)?,
            );
        }
        for (key, bitmap) in posting_updates {
            batch.insert(&self.postings, key, encode_bitmap(&bitmap)?);
        }
        batch
            .commit()
            .context("failed to commit migrated subscription batch")?;
        Ok(count)
    }

    #[cfg(feature = "migration")]
    pub(crate) fn active_subscriptions(&self) -> Result<Vec<StoredSubscription>> {
        let mut records = Vec::new();
        for item in self.subscriptions.iter() {
            let record: StoredSubscription = decode(&item.value()?)?;
            if record.active {
                records.push(record);
            }
        }
        Ok(records)
    }

    #[cfg(feature = "migration")]
    pub(crate) fn verify_posting_consistency(&self) -> Result<()> {
        let mut expected = std::collections::BTreeMap::<[u8; 20], RoaringBitmap>::new();
        for record in self.active_subscriptions()? {
            let compiled = self
                .compiled_subscription(record.id)?
                .context("active subscription is missing its compiled record")?;
            anyhow::ensure!(
                compiled.subscription_id == record.id
                    && compiled.destination_id == record.destination_id
                    && compiled.generation == record.generation,
                "compiled subscription identity mismatch"
            );
            for key in MatchPostingKey::for_subscription(&compiled) {
                expected
                    .entry(key.encode())
                    .or_default()
                    .insert(compiled.subscription_id.posting_offset());
            }
        }
        let mut actual = std::collections::BTreeMap::new();
        for item in self.postings.iter() {
            let (key, value) = item.into_inner()?;
            let key: [u8; 20] = key
                .as_ref()
                .try_into()
                .context("posting key has an invalid length")?;
            actual.insert(key, decode_bitmap(&value)?);
        }
        anyhow::ensure!(
            actual == expected,
            "posting index is not bidirectionally consistent"
        );
        Ok(())
    }

    #[cfg(feature = "migration")]
    pub(crate) fn verify_sample_matches(&self, source: &[Subscription]) -> Result<()> {
        let matcher = crate::matching::MatchEngine::new(1)?;
        for event in crate::matching::sample_events(source, 256) {
            let expected = source
                .iter()
                .filter_map(|subscription| {
                    crate::matching::match_subscription(subscription, &event)
                        .map(|matched| (destination_key(subscription), matched))
                })
                .collect::<std::collections::BTreeMap<_, _>>();
            let plan = MatchPlan::for_event(&event)?;
            let blocks = self.posting_blocks(&plan)?;
            let subscriptions = self.load_compiled_blocks(&blocks)?;
            let rows = matcher.match_blocks(std::sync::Arc::new(event), blocks, &subscriptions);
            let mut actual = std::collections::BTreeMap::new();
            for row in rows {
                let record = self
                    .stored_subscription(row.subscription_id)?
                    .context("sample match references a missing subscription")?;
                actual.insert(
                    destination_key(&record.subscription),
                    crate::matching::ReferenceMatch {
                        target_ordinal: row.target_ordinal,
                        match_kind: row.match_kind,
                        interruption_level: row.interruption_level,
                    },
                );
            }
            anyhow::ensure!(
                actual == expected,
                "sampled matcher result differs from source subscriptions"
            );
        }
        Ok(())
    }

    pub(crate) fn begin_confirmation(
        &self,
        id: u64,
        destination: &crate::models::DestinationId,
        value: Vec<u8>,
    ) -> Result<()> {
        let _lock = self
            .subscription_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall mutation lock poisoned: {error}"))?;
        let destination_key = confirmation_destination_key(destination);
        let previous = self
            .meta
            .get(&destination_key)?
            .map(|value| decode_u64(&value))
            .transpose()?;
        let mut batch = self.db.batch();
        if let Some(previous) = previous {
            batch.remove(&self.meta, confirmation_key(previous));
        }
        batch.insert(&self.meta, confirmation_key(id), value);
        batch.insert(&self.meta, destination_key, id.to_be_bytes());
        batch
            .commit()
            .context("failed to atomically begin confirmation")?;
        Ok(())
    }

    pub(crate) fn replace_confirmation(
        &self,
        id: u64,
        expected: &[u8],
        replacement: Option<Vec<u8>>,
        destination: Option<&crate::models::DestinationId>,
    ) -> Result<bool> {
        let _lock = self
            .subscription_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall mutation lock poisoned: {error}"))?;
        let key = confirmation_key(id);
        let Some(current) = self.meta.get(key)? else {
            return Ok(false);
        };
        if current.as_ref() != expected {
            return Ok(false);
        }
        let mut batch = self.db.batch();
        if let Some(replacement) = replacement {
            batch.insert(&self.meta, key, replacement);
        } else {
            batch.remove(&self.meta, key);
            if let Some(destination) = destination {
                let destination_key = confirmation_destination_key(destination);
                let expected_id = id.to_be_bytes();
                anyhow::ensure!(
                    self.meta
                        .get(&destination_key)?
                        .is_some_and(|value| value.as_ref() == expected_id),
                    "confirmation destination index mismatch"
                );
                batch.remove(&self.meta, destination_key);
            }
        }
        batch
            .commit()
            .context("failed to compare-and-swap confirmation")?;
        Ok(true)
    }

    pub(crate) fn confirmation_record(&self, id: u64) -> Result<Option<Vec<u8>>> {
        Ok(self
            .meta
            .get(confirmation_key(id))?
            .map(|value| value.to_vec()))
    }

    pub(crate) fn confirmation_records(&self) -> Result<Vec<Vec<u8>>> {
        self.meta
            .prefix(b"confirmation:")
            .map(|item| Ok(item.value()?.to_vec()))
            .collect()
    }

    pub(crate) fn remove_confirmation_for_destination(
        &self,
        destination: &crate::models::DestinationId,
    ) -> Result<bool> {
        let _lock = self
            .subscription_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall mutation lock poisoned: {error}"))?;
        let destination_key = confirmation_destination_key(destination);
        let Some(id) = self
            .meta
            .get(&destination_key)?
            .map(|value| decode_u64(&value))
            .transpose()?
        else {
            return Ok(false);
        };
        let mut batch = self.db.batch();
        batch.remove(&self.meta, confirmation_key(id));
        batch.remove(&self.meta, destination_key);
        batch.commit()?;
        Ok(true)
    }

    pub(crate) fn put_context(&self, id: &str, value: Vec<u8>) -> Result<()> {
        self.contexts.insert(id.as_bytes(), value)?;
        Ok(())
    }

    pub(crate) fn context(&self, id: &str) -> Result<Option<Vec<u8>>> {
        Ok(self
            .contexts
            .get(id.as_bytes())?
            .map(|value| value.to_vec()))
    }

    pub(crate) fn context_records(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.contexts
            .iter()
            .map(|item| {
                let (key, value) = item.into_inner()?;
                let id = std::str::from_utf8(&key).context("invalid notification context key")?;
                Ok((id.to_string(), value.to_vec()))
            })
            .collect()
    }

    pub(crate) fn remove_context(&self, id: &str) -> Result<()> {
        self.contexts.remove(id.as_bytes())?;
        Ok(())
    }

    pub(crate) fn posting_blocks(&self, plan: &MatchPlan) -> Result<Vec<PostingBlock>> {
        let mut blocks = Vec::new();
        for scope in &plan.scopes {
            match scope {
                MatchScope::Cells {
                    resolution_index,
                    cells,
                } => {
                    for cell in cells {
                        self.extend_posting_blocks(
                            plan.category,
                            plan.source_id,
                            1 + *resolution_index,
                            *cell,
                            &mut blocks,
                        )?;
                    }
                }
                MatchScope::Regions(regions) => {
                    for region in regions {
                        self.extend_posting_blocks(
                            plan.category,
                            plan.source_id,
                            4,
                            region.0,
                            &mut blocks,
                        )?;
                    }
                }
                MatchScope::Broad => {
                    self.extend_posting_blocks(plan.category, plan.source_id, 0, 0, &mut blocks)?
                }
            }
        }
        let mut merged = std::collections::BTreeMap::<u64, RoaringBitmap>::new();
        for block in blocks {
            *merged.entry(block.id_block).or_default() |= block.ids;
        }
        Ok(merged
            .into_iter()
            .map(|(id_block, ids)| PostingBlock { id_block, ids })
            .collect())
    }

    pub(crate) fn load_compiled_blocks(
        &self,
        blocks: &[PostingBlock],
    ) -> Result<std::collections::HashMap<SubscriptionId, CompiledSubscription>> {
        let capacity = blocks
            .iter()
            .map(|block| usize::try_from(block.ids.len()).unwrap_or(usize::MAX))
            .fold(0usize, usize::saturating_add);
        let mut subscriptions = std::collections::HashMap::with_capacity(capacity);
        for block in blocks {
            for id_low in &block.ids {
                let Some(id) = SubscriptionId::from_posting(block.id_block, id_low) else {
                    continue;
                };
                if let std::collections::hash_map::Entry::Vacant(entry) = subscriptions.entry(id)
                    && let Some(compiled) = self.compiled_subscription(id)?
                {
                    entry.insert(compiled);
                }
            }
        }
        Ok(subscriptions)
    }

    fn extend_posting_blocks(
        &self,
        category: crate::models::DisasterCategory,
        source: crate::subscriptions::SourceId,
        kind: u8,
        value: u64,
        output: &mut Vec<PostingBlock>,
    ) -> Result<()> {
        for selected_source in [Some(source), None] {
            let key = MatchPostingKey {
                category,
                source: selected_source,
                kind,
                value,
                id_block: 0,
            }
            .encode();
            let prefix = &key[..12];
            for item in self.postings.prefix(prefix) {
                let (key, value) = item.into_inner()?;
                let id_block = u64::from_be_bytes(
                    key.get(12..20)
                        .context("posting key is truncated")?
                        .try_into()
                        .context("posting ID block is invalid")?,
                );
                output.push(PostingBlock {
                    id_block,
                    ids: decode_bitmap(&value)?,
                });
            }
        }
        Ok(())
    }

    pub(crate) fn delivery_batch(&self, id: u64) -> Result<Option<DeliveryBatch>> {
        if let Some(value) = get_record(&self.delivery_batches, &delivery_batch_key(0, id))? {
            return Ok(Some(value));
        }
        get_record(&self.delivery_batches, &delivery_batch_key(1, id))
    }

    pub(crate) fn pending_delivery_batches(&self, limit: usize) -> Result<Vec<DeliveryBatch>> {
        self.delivery_batches
            .prefix([0])
            .take(limit)
            .map(|item| decode(&item.value()?))
            .collect()
    }

    pub(crate) fn pending_delivery_batch(&self, id: u64) -> Result<Option<DeliveryBatch>> {
        get_record(&self.delivery_batches, &delivery_batch_key(0, id))
    }

    pub(crate) fn pending_delivery_rows(
        &self,
        delivery_batch_id: u64,
    ) -> Result<Vec<(usize, crate::delivery::DeliveryRow)>> {
        let delivery_batch = self
            .pending_delivery_batch(delivery_batch_id)?
            .context("pending delivery batch is missing")?;
        let progress = self
            .delivery_progress
            .get(delivery_batch_id.to_be_bytes())?
            .map(|value| decode_bitmap(&value))
            .transpose()?
            .unwrap_or_default();
        Ok(delivery_batch
            .rows
            .iter()
            .copied()
            .enumerate()
            .filter(|(row_index, _)| {
                u32::try_from(*row_index).is_ok_and(|value| !progress.contains(value))
            })
            .collect())
    }

    pub(crate) fn delivery_is_destination_head(
        &self,
        destination_id: DestinationNumericId,
        batch_id: u64,
        row_index: u32,
    ) -> Result<bool> {
        let Some(item) = self
            .delivery_by_destination
            .prefix(destination_id.0.to_be_bytes())
            .next()
        else {
            return Ok(false);
        };
        Ok(item.key()?.as_ref() == delivery_destination_key(destination_id, batch_id, row_index))
    }

    pub(crate) fn commit_delivery_lane_outcome(
        &self,
        delivery_batch_id: u64,
        completed_rows: &[u32],
        skipped_rows: &[u32],
        successes: &[DeliverySuccess],
        retries: &[RetryItem],
        dead_letters: &[DeadLetterItem],
    ) -> Result<()> {
        let _lock = self
            .retry_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall mutation lock poisoned: {error}"))?;
        let pending = delivery_batch_key(0, delivery_batch_id);
        let value = self
            .delivery_batches
            .get(pending)?
            .context("delivery batch disappeared before outcome commit")?;
        let delivery_batch: DeliveryBatch = decode(&value)?;
        anyhow::ensure!(
            delivery_batch.id == delivery_batch_id,
            "delivery batch key and record ID differ"
        );
        let mut progress = self
            .delivery_progress
            .get(delivery_batch_id.to_be_bytes())?
            .map(|value| decode_bitmap(&value))
            .transpose()?
            .unwrap_or_default();
        let completed = completed_rows
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        anyhow::ensure!(
            completed.len() == completed_rows.len(),
            "delivery lane contains duplicate completed rows"
        );
        let mut lane_destination = None;
        for row_index in completed_rows {
            let row = delivery_batch
                .rows
                .get(usize::try_from(*row_index).unwrap_or(usize::MAX))
                .context("completed delivery row index is invalid")?;
            anyhow::ensure!(
                lane_destination.is_none_or(|destination| destination == row.destination_id),
                "delivery lane spans multiple destinations"
            );
            lane_destination = Some(row.destination_id);
        }
        let retry_rows = retries
            .iter()
            .map(|retry| retry.row_index)
            .collect::<std::collections::HashSet<_>>();
        anyhow::ensure!(
            retry_rows.len() == retries.len(),
            "delivery lane contains duplicate retries"
        );
        let delivered_at_ms = if successes.is_empty() {
            0
        } else {
            super::try_now_millis()?
        };
        let mut terminal_rows = std::collections::HashSet::new();
        let mut batch = self.db.batch();
        for row_index in skipped_rows {
            anyhow::ensure!(
                completed.contains(row_index) && terminal_rows.insert(*row_index),
                "skipped delivery row does not match its completed row"
            );
        }
        for success in successes {
            let row = delivery_batch
                .rows
                .get(usize::try_from(success.row_index).unwrap_or(usize::MAX))
                .context("delivery success row index is invalid")?;
            anyhow::ensure!(
                completed.contains(&success.row_index)
                    && terminal_rows.insert(success.row_index)
                    && *row == success.row,
                "delivery success does not match its completed row"
            );
            batch.insert(
                &self.ledger,
                ledger_key(
                    &delivery_batch.incident_id,
                    delivery_batch.category,
                    success.row.destination_id.0,
                ),
                encode(&StoredDelivery {
                    delivered_at_ms,
                    event_revision: delivery_batch.event_revision,
                    row: success.row,
                })?,
            );
        }
        for dead_letter in dead_letters {
            let row = delivery_batch
                .rows
                .get(usize::try_from(dead_letter.row_index).unwrap_or(usize::MAX))
                .context("dead-letter row index is invalid")?;
            anyhow::ensure!(
                dead_letter.batch_id == delivery_batch_id
                    && dead_letter.destination_id == row.destination_id
                    && completed.contains(&dead_letter.row_index)
                    && terminal_rows.insert(dead_letter.row_index),
                "dead letter does not match its completed row"
            );
            batch.insert(
                &self.dead_letters,
                dead_letter_key(dead_letter),
                encode(dead_letter)?,
            );
        }
        for retry in retries {
            let row = delivery_batch
                .rows
                .get(usize::try_from(retry.row_index).unwrap_or(usize::MAX))
                .context("retry row index is invalid")?;
            anyhow::ensure!(
                retry.batch_id == delivery_batch_id
                    && retry.destination_id == row.destination_id
                    && completed.contains(&retry.row_index)
                    && terminal_rows.insert(retry.row_index),
                "retry does not match its completed row"
            );
            insert_retry_indexes(self, &mut batch, retry)?;
        }
        anyhow::ensure!(
            terminal_rows == completed,
            "every completed delivery row must have exactly one outcome"
        );
        for row_index in completed_rows {
            let row = delivery_batch
                .rows
                .get(usize::try_from(*row_index).unwrap_or(usize::MAX))
                .context("completed delivery row index is invalid")?;
            anyhow::ensure!(
                progress.insert(*row_index),
                "delivery row was committed more than once"
            );
            if !retry_rows.contains(row_index) {
                batch.remove(
                    &self.delivery_by_destination,
                    delivery_destination_key(row.destination_id, delivery_batch_id, *row_index),
                );
            }
        }
        if progress.len() == delivery_batch.rows.len() as u64 {
            batch.remove(&self.delivery_batches, pending);
            batch.remove(&self.delivery_progress, delivery_batch_id.to_be_bytes());
            let has_retries = !retries.is_empty()
                || self
                    .retries_by_batch
                    .prefix(delivery_batch_id.to_be_bytes())
                    .next()
                    .is_some();
            if has_retries {
                batch.insert(
                    &self.delivery_batches,
                    delivery_batch_key(1, delivery_batch_id),
                    value,
                );
            }
        } else {
            batch.insert(
                &self.delivery_progress,
                delivery_batch_id.to_be_bytes(),
                encode_bitmap(&progress)?,
            );
        }
        batch
            .commit()
            .context("failed to atomically commit delivery lane outcome")?;
        Ok(())
    }

    pub(crate) fn commit_match_batches(
        &self,
        job_id: u64,
        batches: &[DeliveryBatch],
    ) -> Result<()> {
        let _lock = self
            .match_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall matching lock poisoned: {error}"))?;
        let job = self
            .match_job(job_id)?
            .context("MatchJob disappeared before output commit")?;
        anyhow::ensure!(job.id == job_id, "MatchJob key and record ID differ");
        let event = self
            .event(job.event_revision)?
            .context("MatchJob event disappeared before output commit")?;
        let incident = self.incident(&job.incident_id)?;
        let mut batch_ids = std::collections::HashSet::with_capacity(batches.len());
        let mut prepared = Vec::with_capacity(batches.len());
        for delivery_batch in batches {
            anyhow::ensure!(
                delivery_batch.incident_id == job.incident_id
                    && delivery_batch.event_revision == job.event_revision
                    && delivery_batch.category == event.category,
                "delivery batch does not belong to its MatchJob"
            );
            anyhow::ensure!(
                !delivery_batch.rows.is_empty(),
                "delivery batch must contain at least one subscriber row"
            );
            anyhow::ensure!(
                batch_ids.insert(delivery_batch.id),
                "match output contains duplicate delivery batch IDs"
            );
            anyhow::ensure!(
                self.delivery_batch(delivery_batch.id)?.is_none()
                    && self
                        .delivery_progress
                        .get(delivery_batch.id.to_be_bytes())?
                        .is_none(),
                "delivery batch ID already exists"
            );
            let encoded = encode(delivery_batch)?;
            anyhow::ensure!(
                encoded.len() <= crate::delivery::MAX_DELIVERY_BATCH_BYTES,
                "delivery batch exceeds 256 KiB"
            );
            let mut destination_keys = Vec::with_capacity(delivery_batch.rows.len());
            for (row_index, row) in delivery_batch.rows.iter().enumerate() {
                destination_keys.push(delivery_destination_key(
                    row.destination_id,
                    delivery_batch.id,
                    u32::try_from(row_index).context("delivery row index exceeds u32")?,
                ));
            }
            prepared.push((delivery_batch.id, encoded, destination_keys));
        }
        let mut write = self.db.batch();
        if batches.is_empty() {
            write.remove(&self.events, job.event_revision.to_be_bytes());
        }
        if let Some(mut incident) = incident {
            anyhow::ensure!(
                incident.pending_match_jobs > 0,
                "Incident has a MatchJob without a pending-job count"
            );
            incident.pending_match_jobs -= 1;
            if batches.is_empty()
                && !incident.has_matched_subscribers
                && incident.pending_match_jobs == 0
                && self.ledger.prefix(incident.id.as_str()).next().is_none()
            {
                self.remove_incident_indexes(&mut write, &incident)?;
                write.remove(&self.incidents, incident.id.as_str());
            } else {
                incident.has_matched_subscribers |= !batches.is_empty();
                write.insert(&self.incidents, incident.id.as_str(), encode(&incident)?);
            }
        }
        for (delivery_batch_id, encoded, destination_keys) in prepared {
            write.insert(
                &self.delivery_batches,
                delivery_batch_key(0, delivery_batch_id),
                encoded,
            );
            for destination_key in destination_keys {
                write.insert(&self.delivery_by_destination, destination_key, []);
            }
        }
        write.remove(&self.match_jobs, job_id.to_be_bytes());
        write
            .commit()
            .context("failed to atomically commit match output")
    }

    fn remove_incident_indexes(
        &self,
        write: &mut fjall::OwnedWriteBatch,
        incident: &IncidentRecord,
    ) -> Result<()> {
        for reverse in self
            .incident_aliases_by_incident
            .prefix(incident_reverse_prefix(&incident.id))
        {
            let (reverse_key, alias) = reverse.into_inner()?;
            let current = self
                .incident_aliases
                .get(&alias)?
                .context("incident alias reverse index is orphaned")?;
            anyhow::ensure!(
                current.as_ref() == incident.id.as_str().as_bytes(),
                "incident alias reverse index mismatch"
            );
            write.remove(&self.incident_aliases, alias);
            write.remove(&self.incident_aliases_by_incident, reverse_key);
        }
        for reverse in self
            .incident_correlation_by_incident
            .prefix(incident_reverse_prefix(&incident.id))
        {
            let (reverse_key, correlation_key) = reverse.into_inner()?;
            let current = self
                .incident_correlation
                .get(&correlation_key)?
                .context("incident correlation reverse index is orphaned")?;
            anyhow::ensure!(
                current.as_ref() == incident.id.as_str().as_bytes(),
                "incident correlation reverse index mismatch"
            );
            write.remove(&self.incident_correlation, correlation_key);
            write.remove(&self.incident_correlation_by_incident, reverse_key);
        }
        Ok(())
    }

    pub(crate) fn due_retry_heads(&self, now_ms: i64, limit: usize) -> Result<Vec<RetryItem>> {
        let mut heads = Vec::with_capacity(limit);
        let mut previous_destination = None;
        for item in self.retries_by_destination.iter() {
            let (index_key, retry_key_value) = item.into_inner()?;
            let destination = u64::from_be_bytes(
                index_key
                    .get(..8)
                    .context("retry destination index is truncated")?
                    .try_into()
                    .context("retry destination index is invalid")?,
            );
            if previous_destination == Some(destination) {
                continue;
            }
            previous_destination = Some(destination);
            let retry_value = self
                .retries
                .get(&retry_key_value)?
                .context("retry destination index references a missing retry")?;
            let retry: RetryItem = decode(&retry_value)?;
            anyhow::ensure!(
                retry.destination_id.0 == destination
                    && retry_destination_key(&retry).as_slice() == index_key.as_ref(),
                "retry destination index does not match its retry"
            );
            if retry.due_at_ms > now_ms || !self.retry_is_destination_head(&retry)? {
                continue;
            }
            heads.push(retry);
            if heads.len() == limit {
                break;
            }
        }
        Ok(heads)
    }

    pub(crate) fn reschedule_retry(&self, previous: &RetryItem, next: &RetryItem) -> Result<()> {
        let _lock = self
            .retry_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall mutation lock poisoned: {error}"))?;
        anyhow::ensure!(
            previous.id == next.id
                && previous.batch_id == next.batch_id
                && previous.row_index == next.row_index
                && previous.destination_id == next.destination_id,
            "rescheduled retry identity changed"
        );
        let previous_key = retry_key(previous);
        let stored = self
            .retries
            .get(previous_key)?
            .context("retry disappeared before reschedule")?;
        anyhow::ensure!(
            decode::<RetryItem>(&stored)? == *previous,
            "retry changed before reschedule"
        );
        let mut batch = self.db.batch();
        batch.remove(&self.retries, previous_key);
        batch.insert(&self.retries, retry_key(next), encode(next)?);
        batch.insert(
            &self.retries_by_destination,
            retry_destination_key(next),
            retry_key(next),
        );
        batch.insert(
            &self.retries_by_batch,
            retry_batch_key(next),
            retry_key(next),
        );
        batch
            .commit()
            .context("failed to atomically reschedule retry")?;
        Ok(())
    }

    pub(crate) fn complete_retry(&self, retry: &RetryItem) -> Result<()> {
        let _lock = self
            .retry_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall mutation lock poisoned: {error}"))?;
        let key = retry_key(retry);
        if self.retries.get(key)?.is_none() {
            return Ok(());
        }
        self.ensure_retry_current(retry)?;
        let has_other = self
            .retries_by_batch
            .prefix(retry.batch_id.to_be_bytes())
            .any(|item| {
                item.key()
                    .is_ok_and(|key| key.as_ref() != retry_batch_key(retry))
            });
        let mut batch = self.db.batch();
        remove_retry_indexes(self, &mut batch, retry);
        batch.remove(
            &self.delivery_by_destination,
            delivery_destination_key(retry.destination_id, retry.batch_id, retry.row_index),
        );
        if !has_other {
            batch.remove(
                &self.delivery_batches,
                delivery_batch_key(1, retry.batch_id),
            );
        }
        batch
            .commit()
            .context("failed to atomically complete retry")?;
        Ok(())
    }

    pub(crate) fn retry_is_destination_head(&self, candidate: &RetryItem) -> Result<bool> {
        let Some(value) = self.retries.get(retry_key(candidate))? else {
            return Ok(false);
        };
        if decode::<RetryItem>(&value)? != *candidate {
            return Ok(false);
        }
        let Some(item) = self
            .retries_by_destination
            .prefix(candidate.destination_id.0.to_be_bytes())
            .next()
        else {
            return Ok(false);
        };
        Ok(item.key()?.as_ref() == retry_destination_key(candidate)
            && self.delivery_is_destination_head(
                candidate.destination_id,
                candidate.batch_id,
                candidate.row_index,
            )?)
    }

    pub(crate) fn complete_retry_with_success(
        &self,
        retry: &RetryItem,
        success: Option<&DeliverySuccess>,
    ) -> Result<()> {
        let _lock = self
            .retry_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall mutation lock poisoned: {error}"))?;
        self.ensure_retry_current(retry)?;
        let delivery_batch = self
            .delivery_batch(retry.batch_id)?
            .context("retry delivery batch is missing")?;
        let row = delivery_batch
            .rows
            .get(usize::try_from(retry.row_index).unwrap_or(usize::MAX))
            .context("retry row index is invalid")?;
        anyhow::ensure!(
            row.destination_id == retry.destination_id,
            "retry destination does not match its delivery row"
        );
        let mut batch = self.db.batch();
        if let Some(success) = success {
            let delivered_at_ms = super::try_now_millis()?;
            anyhow::ensure!(
                success.row_index == retry.row_index && success.row == *row,
                "retry success does not match its delivery row"
            );
            batch.insert(
                &self.ledger,
                ledger_key(
                    &delivery_batch.incident_id,
                    delivery_batch.category,
                    success.row.destination_id.0,
                ),
                encode(&StoredDelivery {
                    delivered_at_ms,
                    event_revision: delivery_batch.event_revision,
                    row: success.row,
                })?,
            );
        }
        self.complete_retry_in_batch(&mut batch, retry)?;
        batch
            .commit()
            .context("failed to atomically complete successful retry")
    }

    pub(crate) fn dead_letter_retry(
        &self,
        retry: &RetryItem,
        dead_letter: &DeadLetterItem,
    ) -> Result<()> {
        let _lock = self
            .retry_lock
            .lock()
            .map_err(|error| anyhow::anyhow!("Fjall mutation lock poisoned: {error}"))?;
        self.ensure_retry_current(retry)?;
        anyhow::ensure!(
            dead_letter.batch_id == retry.batch_id
                && dead_letter.row_index == retry.row_index
                && dead_letter.destination_id == retry.destination_id,
            "dead letter does not match its retry"
        );
        let mut batch = self.db.batch();
        batch.insert(
            &self.dead_letters,
            dead_letter_key(dead_letter),
            encode(dead_letter)?,
        );
        self.complete_retry_in_batch(&mut batch, retry)?;
        batch
            .commit()
            .context("failed to atomically dead-letter retry")
    }

    fn complete_retry_in_batch(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        retry: &RetryItem,
    ) -> Result<()> {
        let has_other = self
            .retries_by_batch
            .prefix(retry.batch_id.to_be_bytes())
            .any(|item| {
                item.key()
                    .is_ok_and(|key| key.as_ref() != retry_batch_key(retry))
            });
        remove_retry_indexes(self, batch, retry);
        batch.remove(
            &self.delivery_by_destination,
            delivery_destination_key(retry.destination_id, retry.batch_id, retry.row_index),
        );
        if !has_other {
            batch.remove(
                &self.delivery_batches,
                delivery_batch_key(1, retry.batch_id),
            );
        }
        Ok(())
    }

    fn ensure_retry_current(&self, retry: &RetryItem) -> Result<()> {
        let value = self
            .retries
            .get(retry_key(retry))?
            .context("retry disappeared before completion")?;
        anyhow::ensure!(
            decode::<RetryItem>(&value)? == *retry,
            "retry changed before completion"
        );
        Ok(())
    }

    pub(crate) fn delivery_recorded(
        &self,
        incident_id: &IncidentId,
        category: crate::models::DisasterCategory,
        destination_id: DestinationNumericId,
        event_revision: u64,
    ) -> Result<bool> {
        self.ledger
            .get(ledger_key(incident_id, category, destination_id.0))?
            .map(|value| {
                decode::<StoredDelivery>(&value)
                    .map(|delivery| delivery.event_revision == event_revision)
            })
            .transpose()
            .map(Option::unwrap_or_default)
    }

    pub(crate) fn delivered_rows(
        &self,
        incident_id: &IncidentId,
        category: crate::models::DisasterCategory,
    ) -> Result<Vec<crate::delivery::DeliveryRow>> {
        self.ledger
            .prefix(ledger_prefix(incident_id, category))
            .map(|item| decode::<StoredDelivery>(&item.value()?).map(|value| value.row))
            .collect()
    }

    pub(crate) fn prune(
        &self,
        incident_cutoff_ms: i64,
        ledger_cutoff_ms: i64,
        event_cutoff_ms: i64,
    ) -> Result<StoragePruneStats> {
        let mut stats = StoragePruneStats::default();
        let mut write = self.db.batch();

        for item in self.ledger.iter() {
            let (key, value) = item.into_inner()?;
            let delivery: StoredDelivery = decode(&value)?;
            if delivery.delivered_at_ms <= ledger_cutoff_ms {
                write.remove(&self.ledger, key);
                stats.delivery_records = stats.delivery_records.saturating_add(1);
            }
        }

        let mut referenced_incidents = std::collections::HashSet::new();
        let mut referenced_events = std::collections::HashSet::new();
        for item in self.match_jobs.iter() {
            let job: MatchJob = decode(&item.value()?)?;
            referenced_incidents.insert(job.incident_id);
            referenced_events.insert(job.event_revision);
        }
        for item in self.delivery_batches.iter() {
            let batch: DeliveryBatch = decode(&item.value()?)?;
            referenced_incidents.insert(batch.incident_id);
            referenced_events.insert(batch.event_revision);
        }

        for item in self.events.iter() {
            let (key, value) = item.into_inner()?;
            let revision = decode_u64(&key)?;
            let event: DisasterEvent = decode(&value)?;
            let event_at_ms =
                parse_event_epoch(&event).map_or(0, |value| value.saturating_mul(1_000));
            if event_at_ms <= event_cutoff_ms && !referenced_events.contains(&revision) {
                write.remove(&self.events, key);
                stats.events = stats.events.saturating_add(1);
            }
        }

        for item in self.incidents.iter() {
            let (key, value) = item.into_inner()?;
            let incident: IncidentRecord = decode(&value)?;
            if incident.updated_at_ms > incident_cutoff_ms
                || referenced_incidents.contains(&incident.id)
                || self.ledger.prefix(incident.id.as_str()).next().is_some()
            {
                continue;
            }
            self.remove_incident_indexes(&mut write, &incident)?;
            write.remove(&self.incidents, key);
            stats.incidents = stats.incidents.saturating_add(1);
        }
        write
            .commit()
            .context("failed to commit retention pruning")?;
        Ok(stats)
    }
}

fn destination_key(subscription: &Subscription) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"disaster-alert:destination:v1\0");
    hash.update(subscription.bark_base_url().as_bytes());
    hash.update([0]);
    hash.update(subscription.device_key().as_bytes());
    hash.finalize().into()
}

fn destination_digest(destination: &crate::models::DestinationId) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"disaster-alert:destination:v1\0");
    hash.update(destination.base_url.as_bytes());
    hash.update([0]);
    hash.update(destination.device_key.as_bytes());
    hash.finalize().into()
}

fn confirmation_key(id: u64) -> [u8; 21] {
    let mut key = [0; 21];
    key[..13].copy_from_slice(b"confirmation:");
    key[13..].copy_from_slice(&id.to_be_bytes());
    key
}

fn confirmation_destination_key(destination: &crate::models::DestinationId) -> Vec<u8> {
    let mut key = Vec::with_capacity(25 + 32);
    key.extend_from_slice(b"confirmation-destination:");
    key.extend_from_slice(&destination_digest(destination));
    key
}

fn retry_key(retry: &RetryItem) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&retry.due_at_ms.max(0).to_be_bytes());
    key[8..].copy_from_slice(&retry.id.to_be_bytes());
    key
}

fn retry_destination_key(retry: &RetryItem) -> [u8; 28] {
    let mut key = [0; 28];
    key[..8].copy_from_slice(&retry.destination_id.0.to_be_bytes());
    key[8..16].copy_from_slice(&retry.batch_id.to_be_bytes());
    key[16..20].copy_from_slice(&retry.row_index.to_be_bytes());
    key[20..].copy_from_slice(&retry.id.to_be_bytes());
    key
}

fn retry_batch_key(retry: &RetryItem) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&retry.batch_id.to_be_bytes());
    key[8..].copy_from_slice(&retry.id.to_be_bytes());
    key
}

fn delivery_destination_key(
    destination_id: DestinationNumericId,
    batch_id: u64,
    row_index: u32,
) -> [u8; 20] {
    let mut key = [0; 20];
    key[..8].copy_from_slice(&destination_id.0.to_be_bytes());
    key[8..16].copy_from_slice(&batch_id.to_be_bytes());
    key[16..].copy_from_slice(&row_index.to_be_bytes());
    key
}

fn dead_letter_key(dead_letter: &DeadLetterItem) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&dead_letter.failed_at_ms.max(0).to_be_bytes());
    key[8..].copy_from_slice(&dead_letter.id.to_be_bytes());
    key
}

fn insert_retry_indexes(
    storage: &FjallStorage,
    batch: &mut fjall::OwnedWriteBatch,
    retry: &RetryItem,
) -> Result<()> {
    let encoded_key = retry_key(retry);
    batch.insert(&storage.retries, encoded_key, encode(retry)?);
    batch.insert(
        &storage.retries_by_destination,
        retry_destination_key(retry),
        encoded_key,
    );
    batch.insert(
        &storage.retries_by_batch,
        retry_batch_key(retry),
        encoded_key,
    );
    Ok(())
}

fn remove_retry_indexes(
    storage: &FjallStorage,
    batch: &mut fjall::OwnedWriteBatch,
    retry: &RetryItem,
) {
    batch.remove(&storage.retries, retry_key(retry));
    batch.remove(
        &storage.retries_by_destination,
        retry_destination_key(retry),
    );
    batch.remove(&storage.retries_by_batch, retry_batch_key(retry));
}

fn delivery_batch_key(state: u8, id: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = state;
    key[1..].copy_from_slice(&id.to_be_bytes());
    key
}

fn cursor_key(provider: ProviderChannel, stream: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(9 + stream.len());
    key.extend_from_slice(b"cursor:");
    key.push(match provider {
        ProviderChannel::Wolfx => 1,
        ProviderChannel::FanStudio => 2,
        ProviderChannel::Huania => 3,
    });
    key.push(b':');
    key.extend_from_slice(stream.as_bytes());
    key
}

fn ledger_prefix(incident_id: &IncidentId, category: crate::models::DisasterCategory) -> Vec<u8> {
    let mut key = Vec::with_capacity(23);
    key.extend_from_slice(incident_id.as_str().as_bytes());
    key.push(category_code(category));
    key
}

fn ledger_key(
    incident_id: &IncidentId,
    category: crate::models::DisasterCategory,
    destination_id: u64,
) -> Vec<u8> {
    let mut key = ledger_prefix(incident_id, category);
    key.extend_from_slice(&destination_id.to_be_bytes());
    key
}

fn category_code(category: crate::models::DisasterCategory) -> u8 {
    match category {
        crate::models::DisasterCategory::EarthquakeWarning => 1,
        crate::models::DisasterCategory::EarthquakeReport => 2,
        crate::models::DisasterCategory::WeatherWarning => 3,
        crate::models::DisasterCategory::Tsunami => 4,
        crate::models::DisasterCategory::Typhoon => 5,
    }
}

fn incident_alias(event_key: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"disaster-alert:incident-alias:v1\0");
    hash.update(event_key.as_bytes());
    hash.finalize().into()
}

fn incident_alias_reverse_key(id: &IncidentId, alias: &[u8; 32]) -> Vec<u8> {
    let mut key = incident_reverse_prefix(id);
    key.extend_from_slice(alias);
    key
}

fn incident_reverse_prefix(id: &IncidentId) -> Vec<u8> {
    let mut key = Vec::with_capacity(id.as_str().len() + 1);
    key.extend_from_slice(id.as_str().as_bytes());
    key.push(0);
    key
}

fn correlation_key(epoch: i64, id: &str, stream: &[u8; 32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + id.len() + stream.len());
    key.extend_from_slice(&epoch.max(0).to_be_bytes());
    key.extend_from_slice(id.as_bytes());
    key.extend_from_slice(stream);
    key
}

fn correlation_epoch(key: &[u8]) -> Result<i64> {
    Ok(i64::from_be_bytes(
        key.get(..8)
            .context("incident correlation key is truncated")?
            .try_into()
            .context("incident correlation timestamp is invalid")?,
    ))
}

fn correlation_stream(event: &DisasterEvent) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"disaster-alert:incident-correlation-stream:v1\0");
    hash.update([category_code(event.category)]);
    hash.update(event.source.as_bytes());
    hash.update([0]);
    hash.update(event.event_id.as_bytes());
    hash.finalize().into()
}

fn correlation_reverse_key(id: &IncidentId, stream: &[u8; 32]) -> Vec<u8> {
    let mut key = incident_reverse_prefix(id);
    key.extend_from_slice(stream);
    key
}

fn is_earthquake(category: DisasterCategory) -> bool {
    matches!(
        category,
        DisasterCategory::EarthquakeWarning | DisasterCategory::EarthquakeReport
    )
}

fn correlation_score(
    current: &DisasterEvent,
    candidate: &DisasterEvent,
    time_delta_seconds: f64,
) -> Option<f64> {
    if !is_earthquake(current.category) || current.source == candidate.source {
        return None;
    }
    let distance = crate::utils::distance::vincenty_distance(
        current.latitude?,
        current.longitude?,
        candidate.latitude?,
        candidate.longitude?,
    )?;
    if distance > CORRELATION_DISTANCE_KM {
        return None;
    }
    let magnitude_delta = match (current.magnitude, candidate.magnitude) {
        (Some(left), Some(right)) => {
            let delta = (left - right).abs();
            if delta > CORRELATION_MAGNITUDE_DELTA {
                return None;
            }
            delta
        }
        _ => 0.0,
    };
    Some(
        time_delta_seconds / CORRELATION_WINDOW_SECONDS as f64
            + distance / CORRELATION_DISTANCE_KM
            + magnitude_delta / CORRELATION_MAGNITUDE_DELTA,
    )
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    let encoded = encode_record(value)?;
    anyhow::ensure!(
        encoded.len() <= MAX_RECORD_BYTES,
        "Fjall record exceeds storage bound"
    );
    Ok(encoded)
}

fn decode<T: serde::de::DeserializeOwned>(value: &[u8]) -> Result<T> {
    decode_record(value)
}

fn get_record<T: serde::de::DeserializeOwned>(
    keyspace: &Keyspace,
    key: &[u8],
) -> Result<Option<T>> {
    keyspace.get(key)?.map(|value| decode(&value)).transpose()
}

fn decode_u64(value: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(
        value.try_into().context("invalid u64 record")?,
    ))
}

fn encode_bitmap(bitmap: &RoaringBitmap) -> Result<Vec<u8>> {
    let mut encoded = Vec::with_capacity(bitmap.serialized_size());
    bitmap.serialize_into(&mut encoded)?;
    Ok(encoded)
}

fn decode_bitmap(value: &[u8]) -> Result<RoaringBitmap> {
    RoaringBitmap::deserialize_from(Cursor::new(value)).context("invalid posting bitmap")
}

fn insert_postings(
    postings: &Keyspace,
    batch: &mut fjall::OwnedWriteBatch,
    subscription: &CompiledSubscription,
) -> Result<()> {
    for key in MatchPostingKey::for_subscription(subscription) {
        update_posting(
            postings,
            batch,
            key,
            subscription.subscription_id.posting_offset(),
            true,
        )?;
    }
    Ok(())
}

fn remove_postings(
    postings: &Keyspace,
    batch: &mut fjall::OwnedWriteBatch,
    subscription: &CompiledSubscription,
) -> Result<()> {
    for key in MatchPostingKey::for_subscription(subscription) {
        update_posting(
            postings,
            batch,
            key,
            subscription.subscription_id.posting_offset(),
            false,
        )?;
    }
    Ok(())
}

fn update_posting(
    postings: &Keyspace,
    batch: &mut fjall::OwnedWriteBatch,
    key: MatchPostingKey,
    id: u32,
    insert: bool,
) -> Result<()> {
    let encoded_key = key.encode();
    let mut bitmap = postings
        .get(encoded_key)?
        .map(|value| decode_bitmap(&value))
        .transpose()?
        .unwrap_or_default();
    if insert {
        bitmap.insert(id);
    } else {
        bitmap.remove(id);
    }
    if bitmap.is_empty() {
        batch.remove(postings, encoded_key);
    } else {
        batch.insert(postings, encoded_key, encode_bitmap(&bitmap)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delivery::{DeliveryBatch, DeliveryRow, DeliverySuccess, RetryItem};
    use crate::events::EventCoordinator;
    use crate::models::{
        AdministrativeRegion, AlertRule, DisasterCategory, DisasterEvent, GeoPoint, IncidentId,
        IntensityBand, InterruptionLevel, MonitoringTarget, NotificationDestination,
        ProviderChannel, SourceSelection,
    };

    fn subscription() -> Subscription {
        Subscription::new(
            NotificationDestination::Bark {
                base_url: "https://api.day.app".to_string(),
                device_key: "device1".to_string(),
            },
            vec![MonitoringTarget {
                label: "home".to_string(),
                point: GeoPoint {
                    latitude: 31.2,
                    longitude: 121.5,
                },
                region: Default::default(),
            }],
            vec![AlertRule::default_for(DisasterCategory::EarthquakeReport)],
        )
    }

    #[test]
    fn subscription_and_postings_survive_reopen() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let stored = {
            let storage = FjallStorage::open(directory.path())?;
            let stored = storage.store_subscription(subscription())?;
            storage.persist()?;
            stored
        };
        let storage = FjallStorage::open(directory.path())?;
        let compiled = storage
            .compiled_subscription(stored.id)?
            .context("missing compiled subscription")?;
        anyhow::ensure!(compiled.generation == 1);
        let keys = MatchPostingKey::for_subscription(&compiled);
        anyhow::ensure!(keys.into_iter().any(|key| {
            storage.postings.get(key.encode()).is_ok_and(|value| {
                value
                    .and_then(|value| decode_bitmap(&value).ok())
                    .is_some_and(|bitmap| bitmap.contains(stored.id.posting_offset()))
            })
        }));
        Ok(())
    }

    #[test]
    fn empty_recovery_scans_return_no_work() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let storage = FjallStorage::open(directory.path())?;
        anyhow::ensure!(storage.next_match_job()?.is_none());
        anyhow::ensure!(storage.pending_delivery_batches(1)?.is_empty());
        Ok(())
    }

    #[test]
    fn deactivation_removes_compiled_record_and_postings() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let storage = FjallStorage::open(directory.path())?;
        let stored = storage.store_subscription(subscription())?;
        let compiled = storage
            .compiled_subscription(stored.id)?
            .context("missing compiled subscription")?;
        anyhow::ensure!(!storage.postings.is_empty()?);

        anyhow::ensure!(storage.deactivate_subscription(stored.id)?);
        anyhow::ensure!(storage.compiled_subscription(stored.id)?.is_none());
        anyhow::ensure!(storage.postings.is_empty()?);
        let inactive = storage
            .stored_subscription(stored.id)?
            .context("missing subscription tombstone")?;
        anyhow::ensure!(!inactive.active);
        anyhow::ensure!(inactive.generation > compiled.generation);
        Ok(())
    }

    #[test]
    fn bitmap_matcher_agrees_with_reference_matcher_for_generated_cases() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let storage = FjallStorage::open(directory.path())?;
        let mut random = TestRandom(0x5eed_f00d_cafe_babe);
        let mut records = Vec::new();
        for index in 0..64 {
            let category = DisasterCategory::ALL[random.index(DisasterCategory::ALL.len())];
            records.push(storage.store_subscription(generated_subscription(
                &mut random,
                index,
                category,
            ))?);
        }
        let matcher = crate::matching::MatchEngine::new(4)?;
        for index in 0..40 {
            let category = DisasterCategory::ALL[index % DisasterCategory::ALL.len()];
            let event = generated_event(&mut random, index, category);
            let expected = records
                .iter()
                .filter_map(|record| {
                    crate::matching::match_subscription(&record.subscription, &event).map(
                        |matched| {
                            (
                                record.id.0,
                                (
                                    matched.target_ordinal,
                                    matched.match_kind,
                                    matched.interruption_level,
                                ),
                            )
                        },
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>();
            let plan = MatchPlan::for_event(&event)?;
            let blocks = storage.posting_blocks(&plan)?;
            let compiled = storage.load_compiled_blocks(&blocks)?;
            let actual = matcher
                .match_blocks(Arc::new(event), blocks, &compiled)
                .into_iter()
                .map(|row| {
                    (
                        row.subscription_id.0,
                        (row.target_ordinal, row.match_kind, row.interruption_level),
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>();
            anyhow::ensure!(
                actual == expected,
                "generated matcher mismatch for {} case {index}",
                category.as_str()
            );
        }
        Ok(())
    }

    struct TestRandom(u64);

    impl TestRandom {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0
        }

        fn index(&mut self, upper: usize) -> usize {
            usize::try_from(self.next() % upper as u64).unwrap_or(0)
        }

        fn coordinate_offset(&mut self) -> f64 {
            let value = i64::try_from(self.next() % 60_001).unwrap_or(0) - 30_000;
            value as f64 / 10_000.0
        }
    }

    const TEST_REGIONS: [(&str, &str, f64, f64); 4] = [
        ("北京市", "北京市", 39.9042, 116.4074),
        ("上海市", "上海市", 31.2304, 121.4737),
        ("广东省", "广州市", 23.1291, 113.2644),
        ("四川省", "成都市", 30.5728, 104.0668),
    ];

    fn generated_subscription(
        random: &mut TestRandom,
        index: usize,
        category: DisasterCategory,
    ) -> Subscription {
        let source = test_source(category, random.index(3));
        let sources = if random.index(2) == 0 {
            SourceSelection::All
        } else {
            SourceSelection::Include {
                ids: vec![source.id.to_string()],
            }
        };
        let rule = match category {
            DisasterCategory::EarthquakeWarning => AlertRule::EarthquakeWarning {
                sources,
                estimated_intensity_bands: vec![
                    IntensityBand {
                        min: 0,
                        max: 1,
                        interruption_level: InterruptionLevel::Passive,
                    },
                    IntensityBand {
                        min: 2,
                        max: 3,
                        interruption_level: InterruptionLevel::Active,
                    },
                    IntensityBand {
                        min: 4,
                        max: 7,
                        interruption_level: InterruptionLevel::Critical,
                    },
                ],
            },
            DisasterCategory::EarthquakeReport => AlertRule::EarthquakeReport {
                sources,
                min_magnitude: 3.0 + random.index(6) as f64,
            },
            DisasterCategory::WeatherWarning => AlertRule::WeatherWarning {
                sources,
                min_severity: u8::try_from(1 + random.index(4)).unwrap_or(1),
                fallback_radius_km: [80.0, 300.0, 1_000.0][random.index(3)],
            },
            DisasterCategory::Tsunami => AlertRule::Tsunami {
                sources,
                min_severity: u8::try_from(1 + random.index(4)).unwrap_or(1),
            },
            DisasterCategory::Typhoon => AlertRule::Typhoon {
                sources,
                max_center_distance_km: [100.0, 500.0, 1_500.0][random.index(3)],
            },
        };
        let targets = (0..1 + random.index(3))
            .map(|target_index| {
                let (province, city, latitude, longitude) =
                    TEST_REGIONS[random.index(TEST_REGIONS.len())];
                MonitoringTarget {
                    label: format!("target{target_index}"),
                    point: GeoPoint {
                        latitude: latitude + random.coordinate_offset(),
                        longitude: longitude + random.coordinate_offset(),
                    },
                    region: AdministrativeRegion {
                        province: province.to_string(),
                        city: city.to_string(),
                        district: String::new(),
                    },
                }
            })
            .collect();
        Subscription::new(
            NotificationDestination::Bark {
                base_url: "https://api.day.app".to_string(),
                device_key: format!("generated{index:08}"),
            },
            targets,
            vec![rule],
        )
    }

    fn generated_event(
        random: &mut TestRandom,
        index: usize,
        category: DisasterCategory,
    ) -> DisasterEvent {
        let source = test_source(category, random.index(4));
        let (province, city, latitude, longitude) = TEST_REGIONS[random.index(TEST_REGIONS.len())];
        let has_coordinate = category != DisasterCategory::Tsunami || random.index(3) != 0;
        let affected_regions = if random.index(3) == 0 {
            Vec::new()
        } else if random.index(2) == 0 {
            vec![province.to_string()]
        } else {
            vec![city.to_string()]
        };
        DisasterEvent {
            category,
            channel: source.channel,
            source: source.id.to_string(),
            event_id: format!("generated-event-{index}"),
            revision: "1".to_string(),
            report_num: 1,
            title: String::new(),
            description: String::new(),
            latitude: has_coordinate.then(|| latitude + random.coordinate_offset()),
            longitude: has_coordinate.then(|| longitude + random.coordinate_offset()),
            magnitude: Some(2.0 + random.index(8) as f64),
            depth_km: Some(random.index(100) as f64),
            affected_regions,
            radius_km: None,
            level: u8::try_from(1 + random.index(4)).unwrap_or(1),
            occurred_at: "2026-07-13T00:00:00Z".to_string(),
            final_report: false,
            cancel: false,
            training: false,
        }
    }

    fn test_source(
        category: DisasterCategory,
        offset: usize,
    ) -> &'static crate::source_registry::SourceDefinition {
        let sources = crate::source_registry::SOURCES
            .iter()
            .filter(|source| source.category == category)
            .collect::<Vec<_>>();
        sources[offset % sources.len()]
    }

    fn delivery_row(destination: u64) -> DeliveryRow {
        DeliveryRow {
            destination_id: DestinationNumericId(destination),
            subscription_id: SubscriptionId(destination),
            generation: 1,
            target_ordinal: 0,
            match_kind: 1,
            interruption_level: InterruptionLevel::Active,
            distance_m: 1_000,
            intensity_cent: 100,
        }
    }

    fn retry(id: u64, batch_id: u64, row_index: u32, due_at_ms: i64) -> RetryItem {
        RetryItem {
            id,
            batch_id,
            row_index,
            destination_id: DestinationNumericId(u64::from(row_index) + 1),
            due_at_ms,
            attempts: 1,
            created_at_ms: 1,
            last_error: "temporary".to_string(),
        }
    }

    fn stage_match_job(
        storage: &FjallStorage,
        id: u64,
        incident_id: IncidentId,
        event_revision: u64,
        category: DisasterCategory,
    ) -> Result<()> {
        let mut event = correlated_event();
        event.category = category;
        storage
            .events
            .insert(event_revision.to_be_bytes(), encode(&event)?)?;
        storage.match_jobs.insert(
            id.to_be_bytes(),
            encode(&MatchJob {
                id,
                incident_id,
                event_revision,
                created_at_ms: 1,
            })?,
        )?;
        Ok(())
    }

    #[test]
    fn retry_lifecycle_keeps_retained_batch_until_last_row_completes() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let storage = FjallStorage::open(directory.path())?;
        let batch = DeliveryBatch {
            id: 7,
            incident_id: IncidentId::derive("retry-test"),
            event_revision: 3,
            category: DisasterCategory::EarthquakeReport,
            shard: 0,
            created_at_ms: 1,
            rows: vec![delivery_row(1), delivery_row(2)],
        };
        stage_match_job(
            &storage,
            99,
            batch.incident_id.clone(),
            batch.event_revision,
            batch.category,
        )?;
        storage.commit_match_batches(99, std::slice::from_ref(&batch))?;
        let first = retry(1, batch.id, 0, 10);
        let second = retry(2, batch.id, 1, 10);

        storage.commit_delivery_lane_outcome(
            batch.id,
            &[0],
            &[],
            &[],
            std::slice::from_ref(&first),
            &[],
        )?;
        storage.commit_delivery_lane_outcome(
            batch.id,
            &[1],
            &[],
            &[],
            std::slice::from_ref(&second),
            &[],
        )?;
        anyhow::ensure!(storage.pending_delivery_batches(1)?.is_empty());
        anyhow::ensure!(storage.delivery_batch(batch.id)?.is_some());
        anyhow::ensure!(storage.due_retry_heads(10, 10)?.len() == 2);
        anyhow::ensure!(storage.retry_is_destination_head(&first)?);

        storage.complete_retry(&first)?;
        anyhow::ensure!(storage.delivery_batch(batch.id)?.is_some());
        anyhow::ensure!(storage.due_retry_heads(10, 10)? == vec![second.clone()]);
        anyhow::ensure!(!storage.retry_is_destination_head(&first)?);

        let mut rescheduled = second.clone();
        rescheduled.due_at_ms = 50;
        rescheduled.attempts = 2;
        storage.reschedule_retry(&second, &rescheduled)?;
        anyhow::ensure!(storage.due_retry_heads(10, 10)?.is_empty());
        anyhow::ensure!(storage.due_retry_heads(50, 10)? == vec![rescheduled.clone()]);

        storage.complete_retry(&rescheduled)?;
        anyhow::ensure!(storage.delivery_batch(batch.id)?.is_none());
        anyhow::ensure!(storage.due_retry_heads(i64::MAX, 10)?.is_empty());
        Ok(())
    }

    #[test]
    fn delivery_lane_rejects_cross_destination_and_forged_outcomes() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let storage = FjallStorage::open(directory.path())?;
        let batch = DeliveryBatch {
            id: 8,
            incident_id: IncidentId::derive("delivery-invariant-test"),
            event_revision: 3,
            category: DisasterCategory::EarthquakeReport,
            shard: 0,
            created_at_ms: 1,
            rows: vec![delivery_row(1), delivery_row(2)],
        };
        stage_match_job(
            &storage,
            99,
            batch.incident_id.clone(),
            batch.event_revision,
            batch.category,
        )?;
        storage.commit_match_batches(99, std::slice::from_ref(&batch))?;

        anyhow::ensure!(
            storage
                .commit_delivery_lane_outcome(batch.id, &[0, 1], &[], &[], &[], &[])
                .is_err()
        );
        let mut forged = test_success(batch.rows[0]);
        forged.row.destination_id = DestinationNumericId(9);
        anyhow::ensure!(
            storage
                .commit_delivery_lane_outcome(batch.id, &[0], &[], &[forged], &[], &[])
                .is_err()
        );
        let mut forged_retry = retry(3, batch.id, 0, 10);
        forged_retry.destination_id = DestinationNumericId(9);
        anyhow::ensure!(
            storage
                .commit_delivery_lane_outcome(batch.id, &[0], &[], &[], &[forged_retry], &[],)
                .is_err()
        );
        anyhow::ensure!(storage.pending_delivery_rows(batch.id)?.len() == 2);
        anyhow::ensure!(storage.due_retry_heads(10, 10)?.is_empty());
        Ok(())
    }

    #[test]
    fn delivery_ledger_is_idempotent_per_event_revision() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let storage = FjallStorage::open(directory.path())?;
        let incident = IncidentId::derive("ledger-test");
        let row = delivery_row(42);

        let first_batch = DeliveryBatch {
            id: 11,
            incident_id: incident.clone(),
            event_revision: 1,
            category: DisasterCategory::EarthquakeReport,
            shard: 0,
            created_at_ms: 1,
            rows: vec![row],
        };
        stage_match_job(
            &storage,
            1,
            first_batch.incident_id.clone(),
            first_batch.event_revision,
            first_batch.category,
        )?;
        storage.commit_match_batches(1, std::slice::from_ref(&first_batch))?;
        storage.commit_delivery_lane_outcome(
            first_batch.id,
            &[0],
            &[],
            &[test_success(row)],
            &[],
            &[],
        )?;
        anyhow::ensure!(storage.delivery_recorded(
            &incident,
            DisasterCategory::EarthquakeReport,
            row.destination_id,
            1,
        )?);
        anyhow::ensure!(!storage.delivery_recorded(
            &incident,
            DisasterCategory::EarthquakeReport,
            row.destination_id,
            2,
        )?);

        let second_batch = DeliveryBatch {
            id: 12,
            event_revision: 2,
            ..first_batch
        };
        stage_match_job(
            &storage,
            2,
            second_batch.incident_id.clone(),
            second_batch.event_revision,
            second_batch.category,
        )?;
        storage.commit_match_batches(2, std::slice::from_ref(&second_batch))?;
        storage.commit_delivery_lane_outcome(
            second_batch.id,
            &[0],
            &[],
            &[test_success(row)],
            &[],
            &[],
        )?;
        anyhow::ensure!(!storage.delivery_recorded(
            &incident,
            DisasterCategory::EarthquakeReport,
            row.destination_id,
            1,
        )?);
        anyhow::ensure!(storage.delivery_recorded(
            &incident,
            DisasterCategory::EarthquakeReport,
            row.destination_id,
            2,
        )?);
        anyhow::ensure!(
            storage.delivered_rows(&incident, DisasterCategory::EarthquakeReport)? == vec![row]
        );
        Ok(())
    }

    #[test]
    fn durable_pipeline_recovers_each_stage_across_reopen() -> Result<()> {
        let directory = tempfile::tempdir()?;
        {
            let storage = FjallStorage::open(directory.path())?;
            storage.ingest_with_cursor(
                ProviderChannel::FanStudio,
                vec![correlated_event()],
                None,
            )?;
            storage.persist()?;
        }

        let job = {
            let storage = FjallStorage::open(directory.path())?;
            anyhow::ensure!(storage.pending_inbox(2)?.len() == 1);
            let job = crate::events::EventCoordinator::new(storage.clone())
                .process_next()?
                .context("missing recovered MatchJob")?;
            anyhow::ensure!(storage.pending_inbox(1)?.is_empty());
            anyhow::ensure!(storage.pending_match_jobs(2)? == vec![job.clone()]);
            storage.persist()?;
            job
        };

        let row = delivery_row(41);
        let delivery_batch = DeliveryBatch {
            id: 51,
            incident_id: job.incident_id.clone(),
            event_revision: job.event_revision,
            category: DisasterCategory::EarthquakeReport,
            shard: 0,
            created_at_ms: 1,
            rows: vec![row],
        };
        {
            let storage = FjallStorage::open(directory.path())?;
            anyhow::ensure!(storage.next_match_job()? == Some(job.clone()));
            storage.commit_match_batches(job.id, std::slice::from_ref(&delivery_batch))?;
            anyhow::ensure!(storage.next_match_job()?.is_none());
            let recovered_batches = storage.pending_delivery_batches(2)?;
            anyhow::ensure!(recovered_batches.len() == 1);
            anyhow::ensure!(recovered_batches[0].id == delivery_batch.id);
            storage.persist()?;
        }

        let retry = RetryItem {
            id: 61,
            batch_id: delivery_batch.id,
            row_index: 0,
            destination_id: row.destination_id,
            due_at_ms: 10,
            attempts: 1,
            created_at_ms: 1,
            last_error: "temporary".to_string(),
        };
        {
            let storage = FjallStorage::open(directory.path())?;
            let recovered_batches = storage.pending_delivery_batches(2)?;
            anyhow::ensure!(recovered_batches.len() == 1);
            anyhow::ensure!(recovered_batches[0].id == delivery_batch.id);
            storage.commit_delivery_lane_outcome(
                delivery_batch.id,
                &[0],
                &[],
                &[],
                std::slice::from_ref(&retry),
                &[],
            )?;
            anyhow::ensure!(storage.pending_delivery_batches(1)?.is_empty());
            anyhow::ensure!(storage.delivery_batch(delivery_batch.id)?.is_some());
            storage.persist()?;
        }

        {
            let storage = FjallStorage::open(directory.path())?;
            anyhow::ensure!(storage.due_retry_heads(10, 2)? == vec![retry.clone()]);
            anyhow::ensure!(storage.retry_is_destination_head(&retry)?);
            storage.put_context("recovered-context", b"context".to_vec())?;
            let success = test_success(row);
            storage.complete_retry_with_success(&retry, Some(&success))?;
            storage.persist()?;
        }

        let storage = FjallStorage::open(directory.path())?;
        anyhow::ensure!(storage.pending_inbox(1)?.is_empty());
        anyhow::ensure!(storage.pending_match_jobs(1)?.is_empty());
        anyhow::ensure!(storage.pending_delivery_batches(1)?.is_empty());
        anyhow::ensure!(storage.due_retry_heads(i64::MAX, 1)?.is_empty());
        anyhow::ensure!(storage.delivery_batch(delivery_batch.id)?.is_none());
        anyhow::ensure!(storage.delivery_recorded(
            &delivery_batch.incident_id,
            delivery_batch.category,
            row.destination_id,
            delivery_batch.event_revision,
        )?);
        anyhow::ensure!(storage.context("recovered-context")?.is_some());
        Ok(())
    }

    fn test_success(row: DeliveryRow) -> DeliverySuccess {
        DeliverySuccess { row_index: 0, row }
    }

    fn correlated_event() -> DisasterEvent {
        DisasterEvent {
            category: DisasterCategory::EarthquakeReport,
            channel: ProviderChannel::FanStudio,
            source: "fanstudio.cenc".to_string(),
            event_id: "retention-event".to_string(),
            revision: "1".to_string(),
            report_num: 1,
            title: "retention".to_string(),
            description: String::new(),
            latitude: Some(35.0),
            longitude: Some(105.0),
            magnitude: Some(5.0),
            depth_km: Some(10.0),
            affected_regions: Vec::new(),
            radius_km: None,
            level: 2,
            occurred_at: "2026-07-13T00:00:00Z".to_string(),
            final_report: false,
            cancel: false,
            training: false,
        }
    }

    #[test]
    fn unmatched_result_removes_incident_event_and_reverse_indexes() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let storage = FjallStorage::open(directory.path())?;
        storage.ingest_with_cursor(ProviderChannel::FanStudio, vec![correlated_event()], None)?;
        let job = EventCoordinator::new(storage.clone())
            .process_next()?
            .context("missing match job")?;

        anyhow::ensure!(storage.incident(&job.incident_id)?.is_some());
        anyhow::ensure!(storage.event(job.event_revision)?.is_some());
        anyhow::ensure!(storage.incident_aliases.len()? == 1);
        anyhow::ensure!(storage.incident_correlation.len()? == 1);

        storage.commit_match_batches(job.id, &[])?;
        anyhow::ensure!(storage.incident(&job.incident_id)?.is_none());
        anyhow::ensure!(storage.event(job.event_revision)?.is_none());
        anyhow::ensure!(storage.match_job(job.id)?.is_none());
        anyhow::ensure!(storage.incident_aliases.is_empty()?);
        anyhow::ensure!(storage.incident_aliases_by_incident.is_empty()?);
        anyhow::ensure!(storage.incident_correlation.is_empty()?);
        anyhow::ensure!(storage.incident_correlation_by_incident.is_empty()?);
        Ok(())
    }

    #[test]
    fn unmatched_incident_waits_for_its_last_match_job() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let storage = FjallStorage::open(directory.path())?;
        let first = correlated_event();
        let mut second = first.clone();
        second.report_num = 2;
        second.revision = "2".to_string();
        storage.ingest_with_cursor(ProviderChannel::FanStudio, vec![first, second], None)?;
        let coordinator = EventCoordinator::new(storage.clone());
        let first_job = coordinator.process_next()?.context("missing first job")?;
        let second_job = coordinator.process_next()?.context("missing second job")?;
        anyhow::ensure!(first_job.incident_id == second_job.incident_id);

        storage.commit_match_batches(first_job.id, &[])?;
        anyhow::ensure!(storage.incident(&first_job.incident_id)?.is_some());
        anyhow::ensure!(storage.event(first_job.event_revision)?.is_none());
        anyhow::ensure!(storage.event(second_job.event_revision)?.is_some());

        storage.commit_match_batches(second_job.id, &[])?;
        anyhow::ensure!(storage.incident(&first_job.incident_id)?.is_none());
        anyhow::ensure!(storage.event(second_job.event_revision)?.is_none());
        anyhow::ensure!(storage.pending_match_jobs(1)?.is_empty());
        Ok(())
    }

    #[test]
    fn later_empty_result_keeps_an_incident_that_matched_before() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let storage = FjallStorage::open(directory.path())?;
        let first = correlated_event();
        storage.ingest_with_cursor(ProviderChannel::FanStudio, vec![first.clone()], None)?;
        let coordinator = EventCoordinator::new(storage.clone());
        let first_job = coordinator.process_next()?.context("missing first job")?;
        let batch = DeliveryBatch {
            id: storage.next_id("delivery_batch")?,
            incident_id: first_job.incident_id.clone(),
            event_revision: first_job.event_revision,
            category: first.category,
            shard: 0,
            created_at_ms: first_job.created_at_ms,
            rows: vec![delivery_row(1)],
        };
        storage.commit_match_batches(first_job.id, &[batch])?;
        anyhow::ensure!(
            storage
                .incident(&first_job.incident_id)?
                .is_some_and(|incident| incident.has_matched_subscribers)
        );

        let mut update = first;
        update.report_num = 2;
        update.revision = "2".to_string();
        storage.ingest_with_cursor(ProviderChannel::FanStudio, vec![update], None)?;
        let second_job = coordinator.process_next()?.context("missing second job")?;
        storage.commit_match_batches(second_job.id, &[])?;

        anyhow::ensure!(storage.incident(&first_job.incident_id)?.is_some());
        anyhow::ensure!(storage.event(second_job.event_revision)?.is_none());
        anyhow::ensure!(!storage.incident_aliases.is_empty()?);
        Ok(())
    }
}
