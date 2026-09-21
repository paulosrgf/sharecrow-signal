use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
type Tx = mpsc::UnboundedSender<Message>;

#[derive(Default)]
struct Room {
    peers: Vec<Tx>,
}

type Rooms = Arc<Mutex<HashMap<String, Room>>>;

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMsg {
    Create,
    Join { code: String },
    Relay { payload: serde_json::Value },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMsg<'a> {
    Created { code: String },
    Joined,
    PeerJoined,
    PeerLeft,
    Relay { payload: &'a serde_json::Value },
    Error { message: String },
}

fn generate_code() -> String {
    const CHARS: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // sem caracteres ambíguos (0/O, 1/I)
    let mut rng = rand::rng();
    (0..6).map(|_| CHARS[rng.random_range(0..CHARS.len())] as char).collect()
}

fn to_message<T: Serialize>(msg: &T) -> Message {
    Message::Text(serde_json::to_string(msg).unwrap().into())
}

#[tokio::main]
async fn main() {
    let rooms: Rooms = Arc::new(Mutex::new(HashMap::new()));

    let app = Router::new().route("/ws", get(ws_handler)).with_state(rooms);

    // Render injeta a porta dinamicamente via variável de ambiente "PORT"
    // Caso rode local, ele pega o 8787 como padrão.
    let port = std::env::var("PORT").unwrap_or_else(|_| "8787".to_string());
    let addr = format!("0.0.0.0:{}", port);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("sharecrow-signal ouvindo em {}", addr);
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade, State(rooms): State<Rooms>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, rooms))
}

async fn handle_socket(socket: WebSocket, rooms: Rooms) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_tx.send(msg).await.is_err() {
                break;
            }
        }
    });

    let mut current_room: Option<String> = None;

    while let Some(Ok(msg)) = ws_rx.next().await {
        let Message::Text(text) = msg else { continue };

        let Ok(client_msg) = serde_json::from_str::<ClientMsg>(&text) else {
            let _ = tx.send(to_message(&ServerMsg::Error { message: "mensagem inválida".into() }));
            continue;
        };

        match client_msg {
            ClientMsg::Create => {
                let code = generate_code();
                let mut rooms_guard = rooms.lock().await;
                rooms_guard.insert(code.clone(), Room { peers: vec![tx.clone()] });
                current_room = Some(code.clone());
                let _ = tx.send(to_message(&ServerMsg::Created { code }));
            }
            ClientMsg::Join { code } => {
                let mut rooms_guard = rooms.lock().await;
                let Some(room) = rooms_guard.get_mut(&code) else {
                    let _ = tx.send(to_message(&ServerMsg::Error { message: "sala não encontrada".into() }));
                    continue;
                };
                if room.peers.len() >= 2 {
                    let _ = tx.send(to_message(&ServerMsg::Error { message: "sala cheia".into() }));
                    continue;
                }
                room.peers.push(tx.clone());
                current_room = Some(code.clone());
                for peer in &room.peers {
                    let _ = peer.send(to_message(&ServerMsg::PeerJoined));
                }
            }
            ClientMsg::Relay { payload } => {
                let Some(code) = &current_room else { continue };
                let rooms_guard = rooms.lock().await;
                if let Some(room) = rooms_guard.get(code) {
                    for peer in &room.peers {
                        if !peer.same_channel(&tx) {
                            let _ = peer.send(to_message(&ServerMsg::Relay { payload: &payload }));
                        }
                    }
                }
            }
        }
    }

    if let Some(code) = current_room {
        let mut rooms_guard = rooms.lock().await;
        if let Some(room) = rooms_guard.get_mut(&code) {
            room.peers.retain(|p| !p.same_channel(&tx));
            for peer in &room.peers {
                let _ = peer.send(to_message(&ServerMsg::PeerLeft));
            }
            if room.peers.is_empty() {
                rooms_guard.remove(&code);
            }
        }
    }

    let _ = send_task.await;
}