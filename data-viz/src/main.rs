use tracing_subscriber::EnvFilter;

fn data_dir() -> std::path::PathBuf {
    std::env::var_os("BACKTEST_DATA_DIR")
        .filter(|value| !value.is_empty())
        .map(Into::into)
        .expect("BACKTEST_DATA_DIR must point to the canonical dataset root")
}

#[tokio::main]
async fn main() {
    // Default to info so request logs show; RUST_LOG still overrides.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let root = data_dir();
    tracing::info!("data root: {}", root.display());

    let app = data_viz::create_app(root).await;
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(3000);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.unwrap();
    tracing::info!("Listening on http://0.0.0.0:{port}");
    axum::serve(listener, app).await.unwrap();
}
