mod fanstudio;
mod fanstudio_protocol;
mod huania;
mod reconnect;
mod value;
mod wolfx;
mod wolfx_protocol;

use anyhow::Result;
use serde::{Deserialize, Serialize};

const MAX_PROVIDER_CURSOR_VALUE_BYTES: usize = 8 * 1024;

pub(crate) use fanstudio::FanStudioSource;
pub(crate) use huania::HuaniaSource;
pub(crate) use wolfx::WolfxSource;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProviderCursor {
    stream: String,
    value: String,
}

impl ProviderCursor {
    pub(crate) fn new(stream: impl Into<String>, value: impl Into<String>) -> Result<Self> {
        let cursor = Self {
            stream: stream.into(),
            value: value.into(),
        };
        anyhow::ensure!(
            !cursor.stream.is_empty() && cursor.stream.len() <= 128,
            "provider cursor stream must contain 1..=128 bytes"
        );
        anyhow::ensure!(
            !cursor.value.is_empty() && cursor.value.len() <= MAX_PROVIDER_CURSOR_VALUE_BYTES,
            "provider cursor value must contain 1..={MAX_PROVIDER_CURSOR_VALUE_BYTES} bytes"
        );
        Ok(cursor)
    }

    pub(crate) fn stream(&self) -> &str {
        &self.stream
    }

    pub(crate) fn value(&self) -> &str {
        &self.value
    }
}
