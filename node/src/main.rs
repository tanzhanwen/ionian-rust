#[macro_use]
extern crate tracing;

mod cli;
mod client;
mod config;
mod log;

use client::{Client, ClientBuilder, RuntimeContext};
use config::IonianConfig;
use std::error::Error;

async fn start_node(context: RuntimeContext, config: IonianConfig) -> Result<Client, String> {
    let network_config = config.network_config()?;
    let storage_config = config.storage_config()?;
    let rpc_config = config.rpc_config()?;
    let log_sync_config = config.log_sync_config()?;

    ClientBuilder::new()
        .with_runtime_context(context)
        .with_rocksdb_store(&storage_config)?
        .with_file_location_cache()
        .with_network(&network_config)
        .await?
        .with_sync()?
        .with_miner()?
        .with_router()?
        .with_rpc(rpc_config, config.chunk_pool_config())
        .await?
        .with_log_sync(log_sync_config)
        .await?
        .build()
}

fn main() -> Result<(), Box<dyn Error>> {
    // enable backtraces
    std::env::set_var("RUST_BACKTRACE", "1");

    // runtime environment
    let mut environment = client::EnvironmentBuilder::new()
        .multi_threaded_tokio_runtime()?
        .build()?;

    let context = environment.core_context();
    let executor = context.executor.clone();

    // CLI, config, and logs
    let matches = cli::cli_app().get_matches();
    let config = IonianConfig::parse(&matches)?;
    log::configure(&config.log_config_file, executor.clone());

    // start services
    executor.clone().spawn(
        async move {
            info!("Starting services...");
            if let Err(e) = start_node(context.clone(), config).await {
                error!(reason = %e, "Failed to start ionian node");
                // Ignore the error since it always occurs during normal operation when
                // shutting down.
                let _ =
                    executor
                        .shutdown_sender()
                        .try_send(task_executor::ShutdownReason::Failure(
                            "Failed to start ionian node",
                        ));
            } else {
                info!("Services started");
            }
        },
        "ionian_node",
    );

    // Block this thread until we get a ctrl-c or a task sends a shutdown signal.
    let shutdown_reason = environment.block_until_shutdown_requested()?;
    info!(reason = ?shutdown_reason, "Shutting down...");

    environment.fire_signal();

    // Shutdown the environment once all tasks have completed.
    environment.shutdown_on_idle();

    match shutdown_reason {
        task_executor::ShutdownReason::Success(_) => Ok(()),
        task_executor::ShutdownReason::Failure(msg) => Err(msg.to_string().into()),
    }
}
