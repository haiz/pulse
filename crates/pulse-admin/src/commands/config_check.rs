pub fn run(path: &str) -> anyhow::Result<()> {
    let contents = std::fs::read_to_string(path)?;

    // Try parsing as broker config
    let result: Result<serde_yaml::Value, _> = serde_yaml::from_str(&contents);

    match result {
        Ok(value) => {
            println!("Config file: {path}");
            println!("  Status:    VALID");
            if let serde_yaml::Value::Mapping(map) = &value {
                println!("  Keys:      {}", map.len());
                for key in map.keys() {
                    if let serde_yaml::Value::String(k) = key {
                        println!("    - {k}");
                    }
                }
            }
            Ok(())
        }
        Err(e) => {
            println!("Config file: {path}");
            println!("  Status:    INVALID");
            println!("  Error:     {e}");
            Err(anyhow::anyhow!("invalid config: {e}"))
        }
    }
}
