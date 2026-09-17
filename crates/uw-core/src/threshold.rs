use crate::model::*;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ThresholdField {
    NoticePct,
    ClosingPct,
    CompactPct,
    PlanPressurePct,
    PlanPressureMinTokens,
    BurnMultiplier,
    OverheadPct,
    MinLeadMinutes,
    MaxLeadMinutes,
    ReaskDeltaPct,
    ReaskMaxPerEpoch,
    CacheWriteFallbackPct,
}
#[derive(Clone, Debug, Default)]
pub struct ThresholdOverrides(pub HashMap<ThresholdField, ThresholdValue>);
#[derive(Clone, Debug, PartialEq)]
pub enum ThresholdValue {
    Percentage(f32),
    Tokens(u64),
    Count(u32),
    Multiplier(f32),
}
pub struct ThresholdResolver {
    pub global: ThresholdProfile,
    providers: HashMap<Provider, ThresholdOverrides>,
    models: HashMap<(Provider, ModelId), ThresholdOverrides>,
    sessions: HashMap<(Provider, SessionId), ThresholdOverrides>,
}
impl ThresholdResolver {
    pub fn new(global: ThresholdProfile) -> Self {
        Self {
            global,
            providers: HashMap::new(),
            models: HashMap::new(),
            sessions: HashMap::new(),
        }
    }
    pub fn set_provider(&mut self, p: Provider, o: ThresholdOverrides) {
        self.providers.insert(p, o);
    }
    pub fn set_model(&mut self, p: Provider, m: ModelId, o: ThresholdOverrides) {
        self.models.insert((p, m), o);
    }
    pub fn set_session(&mut self, p: Provider, s: SessionId, o: ThresholdOverrides) {
        self.sessions.insert((p, s), o);
    }
    pub fn resolve(&self, scope: &ThresholdScope) -> Result<ThresholdProfile, String> {
        let mut out = self.global.clone();
        for o in [
            self.providers.get(&scope.provider),
            scope
                .model
                .as_ref()
                .and_then(|m| self.models.get(&(scope.provider.clone(), m.clone()))),
            scope
                .session
                .as_ref()
                .and_then(|s| self.sessions.get(&(scope.provider.clone(), s.clone()))),
        ]
        .into_iter()
        .flatten()
        {
            for (f, v) in &o.0 {
                apply(&mut out, *f, v.clone())?;
            }
        }
        Ok(out)
    }
}
fn apply(p: &mut ThresholdProfile, f: ThresholdField, v: ThresholdValue) -> Result<(), String> {
    match (f, v) {
        (ThresholdField::NoticePct, ThresholdValue::Percentage(x)) => p.notice_pct = x,
        (ThresholdField::ClosingPct, ThresholdValue::Percentage(x)) => p.closing_pct = x,
        (ThresholdField::CompactPct, ThresholdValue::Percentage(x)) => p.compact_pct = x,
        (ThresholdField::PlanPressurePct, ThresholdValue::Percentage(x)) => p.plan_pressure_pct = x,
        (ThresholdField::PlanPressureMinTokens, ThresholdValue::Tokens(x)) => {
            p.plan_pressure_min_tokens = x
        }
        (ThresholdField::BurnMultiplier, ThresholdValue::Multiplier(x)) => p.burn_multiplier = x,
        (ThresholdField::OverheadPct, ThresholdValue::Percentage(x)) => p.overhead_pct = x,
        (ThresholdField::MinLeadMinutes, ThresholdValue::Percentage(x)) => p.min_lead_minutes = x,
        (ThresholdField::MaxLeadMinutes, ThresholdValue::Percentage(x)) => p.max_lead_minutes = x,
        (ThresholdField::ReaskDeltaPct, ThresholdValue::Percentage(x)) => p.reask_delta_pct = x,
        (ThresholdField::ReaskMaxPerEpoch, ThresholdValue::Count(x)) => p.reask_max_per_epoch = x,
        (ThresholdField::CacheWriteFallbackPct, ThresholdValue::Percentage(x)) => {
            p.cache_write_fallback_pct = x
        }
        (field, value) => {
            return Err(format!(
                "threshold override type mismatch for {field:?}: {value:?}"
            ));
        }
    }
    Ok(())
}

// TODO: Idle-compact tiers, reseed config, and cache TTLs need richer ThresholdValue variants.

#[cfg(test)]
mod tests {
    use super::*;
    fn o(v: f32) -> ThresholdOverrides {
        ThresholdOverrides(HashMap::from([(
            ThresholdField::ClosingPct,
            ThresholdValue::Percentage(v),
        )]))
    }
    #[test]
    fn resolution_is_field_specific_and_most_specific_wins() {
        let p = Provider::Codex;
        let m = ModelId("m".into());
        let s = SessionId("s".into());
        let mut r = ThresholdResolver::new(ThresholdProfile::default());
        r.set_provider(p.clone(), o(71.));
        r.set_model(p.clone(), m.clone(), o(72.));
        r.set_session(p.clone(), s.clone(), ThresholdOverrides::default());
        assert_eq!(
            r.resolve(&ThresholdScope {
                provider: p.clone(),
                model: Some(m.clone()),
                session: Some(s.clone())
            })
            .unwrap()
            .closing_pct,
            72.
        );
        r.set_session(p.clone(), s, o(73.));
        assert_eq!(
            r.resolve(&ThresholdScope {
                provider: p,
                model: None,
                session: None
            })
            .unwrap()
            .closing_pct,
            71.
        );
    }

    #[test]
    fn session_override_is_independent_of_model_resolution() {
        let p = Provider::Codex;
        let s = SessionId("s".into());
        let mut r = ThresholdResolver::new(ThresholdProfile::default());
        r.set_session(p.clone(), s.clone(), o(73.));
        assert_eq!(
            r.resolve(&ThresholdScope {
                provider: p.clone(),
                model: Some(ModelId("known".into())),
                session: Some(s.clone())
            })
            .unwrap()
            .closing_pct,
            73.
        );
        r.set_session(p.clone(), s.clone(), o(74.));
        assert_eq!(
            r.resolve(&ThresholdScope {
                provider: p,
                model: None,
                session: Some(s)
            })
            .unwrap()
            .closing_pct,
            74.
        );
    }

    #[test]
    fn type_mismatch_is_reported() {
        let mut r = ThresholdResolver::new(ThresholdProfile::default());
        r.set_provider(
            Provider::Codex,
            ThresholdOverrides(HashMap::from([(
                ThresholdField::ClosingPct,
                ThresholdValue::Tokens(1),
            )])),
        );
        assert!(
            r.resolve(&ThresholdScope {
                provider: Provider::Codex,
                model: None,
                session: None
            })
            .is_err()
        );
    }
}
