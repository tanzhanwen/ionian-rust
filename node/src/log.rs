use tracing_subscriber::EnvFilter;

pub fn configure(logfile: &str) {
    let builder = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_env_filter(EnvFilter::default())
        // .with_file(true)
        // .with_line_number(true)
        // .with_thread_names(true)
        .with_filter_reloading();

    let handle = builder.reload_handle();
    builder.init();

    let logfile = logfile.to_string();

    tokio::spawn(async move {
        let mut config = "".to_string();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

        loop {
            interval.tick().await;

            let new_config = match tokio::fs::read_to_string(&logfile).await {
                Ok(c) if c == config => continue,
                Ok(c) => c,
                Err(e) => {
                    println!("Unable to read log file {}: {:?}", logfile, e);
                    continue;
                }
            };

            println!("Updating log config to {:?}", new_config);

            match handle.reload(&new_config) {
                Ok(()) => config = new_config,
                Err(e) => {
                    println!("Failed to load new config: {:?}", e);
                }
            }
        }
    });
}
