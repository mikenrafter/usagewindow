use uw_core::model::Provider;
use uw_store::Store;

#[test]
fn inhibits_default_off_compose_and_persist_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let path = path.to_str().unwrap();
    let store = Store::open(path).unwrap();
    assert!(!store.actions_inhibited(&Provider::Codex).unwrap());
    store.set_action_inhibit(Some(&Provider::Codex), true).unwrap();
    assert!(store.actions_inhibited(&Provider::Codex).unwrap());
    assert!(!store.actions_inhibited(&Provider::ClaudeCode).unwrap());
    store.set_action_inhibit(None, true).unwrap();
    store.set_action_inhibit(Some(&Provider::ClaudeCode), false).unwrap();
    assert!(store.actions_inhibited(&Provider::ClaudeCode).unwrap());
    drop(store);
    let store = Store::open(path).unwrap();
    assert!(store.action_inhibits().unwrap().global);
    store.set_action_inhibit(None, false).unwrap();
    assert!(store.actions_inhibited(&Provider::Codex).unwrap());
    assert!(!store.actions_inhibited(&Provider::ClaudeCode).unwrap());
    store.set_action_inhibit(Some(&Provider::Codex), false).unwrap();
    drop(store);
    let store = Store::open_read_only(path).unwrap();
    assert!(!store.actions_inhibited(&Provider::Codex).unwrap());
}
