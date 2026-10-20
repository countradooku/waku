//! In-memory stand-ins for tests (and for wiring the orchestrator up before
//! the real implementations exist).

use std::collections::HashMap;

use parking_lot::Mutex;
use waku_protocol::orchestration::*;

use crate::decide::CapabilitySource;

/// Capabilities per instance, set by the test.
#[derive(Default)]
pub struct StaticCapabilities(pub Mutex<HashMap<ProviderInstanceId, Capabilities>>);

impl StaticCapabilities {
    pub fn with(instance: &str, capabilities: Capabilities) -> Self {
        let map = HashMap::from([(ProviderInstanceId::new(instance), capabilities)]);
        Self(Mutex::new(map))
    }
}

impl CapabilitySource for StaticCapabilities {
    fn for_instance(&self, instance: &ProviderInstanceId) -> Option<Capabilities> {
        self.0.lock().get(instance).cloned()
    }
}
