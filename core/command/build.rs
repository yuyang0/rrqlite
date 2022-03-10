use std::io::Result;
fn main() -> Result<()> {
    let mut config = prost_build::Config::new();
    config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    config.type_attribute(".", "#[serde(rename_all = \"camelCase\")]");
    // config.type_attribute(
    //     "command.parameter.Value",
    //     "#[derive(serde::Serialize, serde::Deserialize)]",
    // );
    // config.type_attribute(
    //     "command.Parameter",
    //     "#[derive(serde::Serialize, serde::Deserialize)]",
    // );
    // config.type_attribute(
    //     "command.Statment",
    //     "#[derive(serde::Serialize, serde::Deserialize)]",
    // );
    // config.type_attribute(
    //     "command.Request",
    //     "#[derive(serde::Serialize, serde::Deserialize)]",
    // );
    // config.type_attribute(
    //     "command.ExecuteRequest",
    //     "#[derive(serde::Serialize, serde::Deserialize)]",
    // );
    config.compile_protos(&["src/command.proto"], &["src/"])?;
    Ok(())
}
