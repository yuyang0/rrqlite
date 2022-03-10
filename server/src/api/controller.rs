use core_command::command;
use core_exception::{ErrorCode, Result};
use core_store::store::Store;
use serde_json::Number;
use serde_json::Value;

pub struct Controller {
    pub s: Store,
}

impl Controller {
    pub fn new(s: Store) -> Self {
        Self { s: s }
    }
    pub fn parse_sql_req(data: &str) -> Result<Vec<command::Statement>> {
        fn parse_parameter_val(jv: &Value) -> Result<command::parameter::Value> {
            let cv = match jv {
                Value::Bool(bv) => command::parameter::Value::B(*bv),
                Value::Number(nv) => {
                    if nv.is_f64() {
                        let fv = nv.as_f64().unwrap();
                        command::parameter::Value::D(fv)
                    } else if nv.is_i64() {
                        let iv = nv.as_i64().unwrap();
                        command::parameter::Value::I(iv)
                    } else if nv.is_u64() {
                        let uv = nv.as_u64().unwrap();
                        command::parameter::Value::I(uv as i64)
                    } else {
                        return Err(ErrorCode::BadRequest("invalid sql request"));
                    }
                }
                Value::String(sv) => command::parameter::Value::S(String::from(sv)),
                _ => return Err(ErrorCode::BadRequest("invalid sql request")),
            };
            Ok(cv)
        }

        fn parse_parameter(jv: &Value, params: &mut Vec<command::Parameter>) -> Result<()> {
            match jv {
                Value::Object(ov) => {
                    // named parameter
                    for (k, v) in ov {
                        let pv = parse_parameter_val(&v)?;
                        let p = command::Parameter {
                            name: String::from(k),
                            value: Some(pv),
                        };
                        params.push(p);
                    }
                }
                _ => {
                    let pv = parse_parameter_val(&*jv)?;
                    let p = command::Parameter {
                        value: Some(pv),
                        ..Default::default()
                    };
                    params.push(p);
                }
            };
            Ok(())
        }

        let v: Value = serde_json::from_str(data)?;
        let mut stmts = vec![];

        match v {
            Value::Array(arr) => {
                for item in arr.iter() {
                    let stmt = match item {
                        Value::Array(arr1) => {
                            if arr1.len() < 1 {
                                return Err(ErrorCode::BadRequest("need SQL statment"));
                            }
                            let mut stmt = command::Statement::default();
                            match arr1[0].as_str() {
                                Some(sql) => stmt.sql = String::from(sql),
                                None => return Err(ErrorCode::BadRequest("need SQL statment")),
                            };
                            for item1 in arr1[1..].iter() {
                                parse_parameter(item1, &mut stmt.parameters)?
                            }
                            stmt
                        }
                        Value::String(sv) => command::Statement {
                            sql: String::from(sv),
                            ..Default::default()
                        },
                        _ => return Err(ErrorCode::BadRequest("invalid sql request")),
                    };
                    stmts.push(stmt);
                }
            }
            _ => return Err(ErrorCode::BadRequest("invalid sql request")),
        }
        Ok(stmts)
    }
}
