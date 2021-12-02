use anyhow::Result;
use axum::{
  extract::{
    ws::{Message, WebSocket, WebSocketUpgrade},
    TypedHeader,
  },
  http::StatusCode,
  response::IntoResponse,
  routing::{get, get_service},
  Router,
};
use tracing::info;
use std::net::SocketAddr;
use tower_http::{
  services::ServeDir,
  trace::{DefaultMakeSpan, TraceLayer},
};

#[tokio::main]
async fn main() -> Result<()> {
  let app = Router::new()
    .fallback(
      get_service(
        ServeDir::new("assets").append_index_html_on_directories(true),
      )
      .handle_error(|error: std::io::Error| async move {
        (
          StatusCode::INTERNAL_SERVER_ERROR,
          format!("Unhandled internal error: {}", error),
        )
      }),
    )
    .route("/stream", get(ws_handler))
    .layer(
      TraceLayer::new_for_http()
        .make_span_with(DefaultMakeSpan::default().include_headers(true)),
    );

  let addr = SocketAddr::from(([0, 0, 0, 0], 80));
  info!("Audit node UI listening on {}", addr);
  axum::Server::bind(&addr)
    .serve(app.into_make_service())
    .await
    .unwrap();
  Ok(())
}

async fn ws_handler(
  ws: WebSocketUpgrade,
  user_agent: Option<TypedHeader<headers::UserAgent>>,
) -> impl IntoResponse {
  if let Some(TypedHeader(user_agent)) = user_agent {
    println!("`{}` connected", user_agent.as_str());
  }

  ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
  if let Some(msg) = socket.recv().await {
    if let Ok(msg) = msg {
      println!("Client says: {:?}", msg);
    } else {
      println!("client disconnected");
      return;
    }
  }

  loop {
    if socket
      .send(Message::Text(String::from("Hi!")))
      .await
      .is_err()
    {
      println!("client disconnected");
      return;
    }
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
  }
}
