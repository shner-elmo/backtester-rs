use tracing_subscriber::EnvFilter;

fn data_root() -> String {
    // CLI arg takes precedence over DATA_DIR env var
    std::env::args()
        .nth(1)
        .or_else(|| std::env::var("DATA_DIR").ok())
        .unwrap_or_else(|| "../../data/output".to_string())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let root = data_root();
    tracing::info!("data root: {}", root);

    let app = data_viz::create_app(root).await;
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(3000);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.unwrap();
    tracing::info!("Listening on http://0.0.0.0:{port}");
    axum::serve(listener, app).await.unwrap();
}
