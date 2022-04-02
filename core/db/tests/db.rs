use bytes::BufMut;
use core_command::command::{self, QueryResult};
use core_db::db::{Context, DB};
use core_db::error::Result;

fn prepare_db(db: &DB) -> Result<()> {
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

fn check_initial_data(db: &DB) -> Result<()> {
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
    let t = tempfile::tempdir().expect("create temp dir to sled db");
    let db_path = t.path().join("test-tmp.db");
    if use_mem {
        cur_db = DB::new_mem_db()?;
    } else {
        cur_db = DB::new_disk_db(db_path, true)?;
    }
    prepare_db(&mut cur_db)?;
    check_initial_data(&cur_db)?;
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
    let t = tempfile::tempdir().expect("create temp dir to sled db");
    let db_path = t.path().join("test-tmp.db");
    let mut disk_db = DB::new_disk_db(db_path.clone(), true)?;
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

    let t = tempfile::tempdir().expect("create temp dir to sled db");
    let db_path = t.path().join("test-tmp.db");
    let mut disk_db = DB::new_disk_db(db_path, true)?;
    disk_db.deserialize(&data)?;

    check_initial_data(&mut disk_db)?;
    Ok(())
}

#[test]
fn test_dump_sql() -> Result<()> {
    let mut mem_db = DB::new_mem_db()?;
    prepare_db(&mut mem_db)?;

    let mut buf = Vec::with_capacity(1024).writer();

    let ctx = Context::default();
    mem_db.backup_to_sql(&ctx, &mut buf)?;
    let s = String::from_utf8(buf.into_inner()).unwrap();
    println!("+++++++ {}", s);
    Ok(())
}

#[test]
fn test_agg() -> Result<()> {
    let mut mem_db = DB::new_mem_db()?;
    prepare_db(&mut mem_db)?;

    let ctx = Context::default();
    let res = mem_db.query_str_stmt(&ctx, "SELECT COUNT(*) FROM contacts")?;
    println!("agg ++++ {:?}", res);
    let res = res.results;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].values.len(), 1);
    assert_eq!(
        res[0].values[0].parameters[0].value,
        Some(command::parameter::Value::I(2))
    );
    Ok(())
}

#[test]
fn test_pragma() -> Result<()> {
    let mut mem_db = DB::new_mem_db()?;
    prepare_db(&mut mem_db)?;

    let ctx = Context::default();
    // use select
    let res = mem_db.query_str_stmt(&ctx, "SELECT * FROM pragma_table_info('contacts')")?;
    println!("agg ++++ {:?}", res);
    fn check_res(res: QueryResult) {
        let res = res.results;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].values.len(), 4);
        assert_eq!(
            res[0].values[0].parameters[1].value,
            Some(command::parameter::Value::S(String::from("contact_id")))
        );
        assert_eq!(
            res[0].values[1].parameters[1].value,
            Some(command::parameter::Value::S(String::from("name")))
        );
        assert_eq!(
            res[0].values[2].parameters[1].value,
            Some(command::parameter::Value::S(String::from("email")))
        );
        assert_eq!(
            res[0].values[3].parameters[1].value,
            Some(command::parameter::Value::S(String::from("data")))
        );
    }
    check_res(res);

    let res = mem_db.query_str_stmt(&ctx, "pragma table_info('contacts')")?;
    println!("agg ++++ {:?}", res);
    check_res(res);

    Ok(())
}
