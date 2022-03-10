use core_db::db::{Context, DB};
use core_exception::Result;
use scopeguard::defer;
use std::fs;

fn prepare_db(db: &mut DB) -> Result<()> {
    let sql = "
BEGIN;
CREATE TABLE contacts (
    contact_id INTEGER PRIMARY KEY,
    name text NOT NULL,
    email TEXT NOT NULL UNIQUE,
    data BLOB
);
INSERT INTO contacts (contact_id, name, email, data) VALUES (1, 'Jim', 'haha@qq.com', '37e79');
INSERT INTO contacts (contact_id, name, email) VALUES (2, 'Lucy', 'kaka@qq.com');
COMMIT;
    ";
    db.execute_batch(sql)?;
    Ok(())
}

fn check_initial_data(db: &mut DB) -> Result<()> {
    let ctx = Context::default();
    let res = db.query_str_stmt(&ctx, "SELECT contact_id, name, email FROM contacts")?;
    let res = res.results;
    assert_eq!(res.len(), 1);

    let rows = &res[0];
    assert_eq!(rows.values.len(), 2);

    let right_colums = vec!["contact_id", "name", "email"];
    assert_eq!(rows.columns.len(), right_colums.len());
    for (idx, c) in rows.columns.iter().enumerate() {
        assert_eq!(c, &right_colums[idx]);
    }
    for value in rows.values.iter() {
        println!("{:?}", value);
    }
    Ok(())
}

fn test_db(use_mem: bool) -> Result<()> {
    let mut cur_db;
    let db_path = "/tmp/test.db";
    if use_mem {
        cur_db = DB::new_mem_db()?;
    } else {
        cur_db = DB::new_disk_db(db_path, true)?;
    }
    defer! {
        let _ = fs::remove_file(db_path);
    }
    prepare_db(&mut cur_db)?;
    check_initial_data(&mut cur_db)?;
    Ok(())
}

#[test]
fn test_mem_db() -> Result<()> {
    test_db(true)
}

#[test]
fn test_disk_db() -> Result<()> {
    test_db(false)
}

#[test]
fn test_load_mem_db() -> Result<()> {
    let db_path = "/tmp/haha.db";
    defer! {
        let _ = fs::remove_file(db_path);
    }
    let mut disk_db = DB::new_disk_db(db_path, true)?;
    prepare_db(&mut disk_db)?;
    let mut mem_db = DB::load_into_mem(db_path)?;
    check_initial_data(&mut mem_db)?;
    Ok(())
}

#[test]
fn test_serialize_deserialize() -> Result<()> {
    let mut mem_db = DB::new_mem_db()?;
    prepare_db(&mut mem_db)?;
    let data = mem_db.serialize()?;
    mem_db.close()?;
    let db_path = "/tmp/kaka.db";
    defer! {
        let _ = fs::remove_file(db_path);
    }
    let mut disk_db = DB::new_disk_db(db_path, true)?;
    disk_db.deserialize(&data)?;

    check_initial_data(&mut disk_db)?;
    Ok(())
}
