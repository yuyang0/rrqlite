use core_sled::openraft;
use core_store::errors::StoreResult;
use core_store::fsm::{SQLFsm, FSM};
use core_store::types::openraft::{EffectiveMembership, LogId, Membership};

fn test_fsm_helper(fsm: SQLFsm) -> StoreResult<()> {
    let log_id = LogId {
        leader_id: openraft::LeaderId {
            term: 10,
            node_id: 20,
        },
        index: 100,
    };
    fsm.set_last_applied(&log_id)?;
    let new_applied = fsm.get_last_applied()?;
    assert!(new_applied.is_some());
    let new_applied = new_applied.unwrap();
    assert!(log_id == new_applied);

    let mem = EffectiveMembership::new(log_id, Membership::new(vec![], None));
    fsm.set_membership(&mem)?;
    let new_mem = fsm.get_membership()?;
    assert!(new_mem.is_some());
    let new_mem = new_mem.unwrap();
    assert_eq!(mem, new_mem);
    Ok(())
}
#[test]
fn test_mem_fsm() -> StoreResult<()> {
    let fsm = SQLFsm::new::<String>(None)?;
    test_fsm_helper(fsm)
}

#[test]
fn test_disk_fsm() -> StoreResult<()> {
    let t = tempfile::tempdir().expect("create temp dir to sled db");
    let db_path = t.path().join("test-fsm.db");
    let fsm = SQLFsm::new(Some(db_path))?;
    test_fsm_helper(fsm)
}
