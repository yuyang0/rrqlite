use command::{Parameter, QueryResult, QueryRows, Values};
use core_command::command;

#[test]
fn test_json() {
    let qs = QueryResult {
        results: vec![QueryRows {
            columns: vec!["c1".to_string(), "c2".to_string()],
            types: vec!["Integer".to_string(), "Text".to_string()],
            values: vec![Values {
                parameters: vec![
                    Parameter {
                        name: "c1".to_string(),
                        value: Some(command::parameter::Value::I(10)),
                    },
                    Parameter {
                        name: "c2".to_string(),
                        value: Some(command::parameter::Value::S("c2_val".to_string())),
                    },
                ],
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let res = serde_json::to_string(&qs);
    assert!(res.is_ok());
    let json_s = res.unwrap();
    println!("+++ {}", json_s);
    let res = serde_json::from_str::<QueryResult>(&json_s);
    assert!(res.is_ok());
    assert_eq!(qs, res.unwrap());
}
