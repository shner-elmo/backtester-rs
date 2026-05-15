use axum::{Router, routing::get};

#[tokio::main]
async fn main() {
    let app = Router::new().route("/", get(|| async { "data-viz — not yet implemented" }));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    println!("Listening on http://localhost:3001");
    axum::serve(listener, app).await.unwrap();
}
