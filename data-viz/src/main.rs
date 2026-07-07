use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let data_root = std::env::var("DATA_PATH").unwrap_or_else(|_| "../../data/output".to_string());

    let app = data_viz::create_app(data_root).await;
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(3000);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.unwrap();
    tracing::info!("Listening on http://0.0.0.0:{port}");
    axum::serve(listener, app).await.unwrap();
}
