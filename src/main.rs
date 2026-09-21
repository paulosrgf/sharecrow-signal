use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
// CORREÇÃO: Usar rand::Rng ao invés de RngExt
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

type Tx = mpsc::UnboundedSender<Message>;

struct Peer {
    id: String,
    tx: Tx,
}

#[derive(Default)]
struct Room {
    peers: Vec<Peer>,
}

type Rooms = Arc<Mutex<HashMap<String, Room>>>;

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMsg {
    Create,
    Join {
        code: String,
    },
    Relay {
        #[serde(default)]
        target_peer_id: Option<String>,
        payload: serde_json::Value,
    },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMsg<'a> {
    Created {
        code: String,
    },
    Joined,
    RoomUpdate {
        count: usize,
    },
    PeerJoined {
        peer_id: String,
    },
    PeerLeft {
        peer_id: String,
    },
    Relay {
        peer_id: String,
        payload: &'a serde_json::Value,
    },
    Error {
        message: String,
    },
}

fn generate_code() -> String {
    const CHARS: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let mut rng = rand::rng();
    (0..6)
        // CORREÇÃO: random_range está disponível trazendo rand::Rng para o escopo
        .map(|_| CHARS[rng.random_range(0..CHARS.len())] as char)
        .collect()
}

fn to_message<T: Serialize>(msg: &T) -> Message {
    // CORREÇÃO: Simplificado para evitar problemas de tipos de string do Axum
    Message::Text(serde_json::to_string(msg).unwrap().into())
}

#[tokio::main]
async fn main() {
    let rooms: Rooms = Arc::new(Mutex::new(HashMap::new()));

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(rooms);

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

    let peer_id = uuid::Uuid::new_v4().to_string();
    let mut current_room: Option<String> = None;

    while let Some(Ok(msg)) = ws_rx.next().await {
        // CORREÇÃO: Garante que apenas mensagens de texto sejam processadas,
        // ignorando pings/pongs sem quebrar o loop.
        let Message::Text(text) = msg else { continue };

        let Ok(client_msg) = serde_json::from_str::<ClientMsg>(&text) else {
            let _ = tx.send(to_message(&ServerMsg::Error {
                message: "mensagem inválida".into(),
            }));
            continue;
        };

        match client_msg {
            ClientMsg::Create => {
                let code = generate_code();
                let mut rooms_guard = rooms.lock().await;

                rooms_guard.insert(
                    code.clone(),
                    Room {
                        peers: vec![Peer {
                            id: peer_id.clone(),
                            tx: tx.clone(),
                        }],
                    },
                );

                current_room = Some(code.clone());

                let _ = tx.send(to_message(&ServerMsg::Created { code }));
                let _ = tx.send(to_message(&ServerMsg::RoomUpdate { count: 1 }));
            }
            ClientMsg::Join { code } => {
                let mut rooms_guard = rooms.lock().await;

                let Some(room) = rooms_guard.get_mut(&code) else {
                    let _ = tx.send(to_message(&ServerMsg::Error {
                        message: "sala não encontrada".into(),
                    }));
                    continue;
                };

                room.peers.push(Peer {
                    id: peer_id.clone(),
                    tx: tx.clone(),
                });

                current_room = Some(code.clone());
                let count = room.peers.len();
                let new_peer_id = peer_id.clone();

                let _ = tx.send(to_message(&ServerMsg::Joined));

                for peer in &room.peers {
                    if peer.id != peer_id {
                        let _ = peer.tx.send(to_message(&ServerMsg::PeerJoined {
                            peer_id: new_peer_id.clone(),
                        }));
                    }
                    let _ = peer.tx.send(to_message(&ServerMsg::RoomUpdate { count }));
                }
            }
            ClientMsg::Relay {
                target_peer_id,
                payload,
            } => {
                let Some(code) = &current_room else { continue };
                let rooms_guard = rooms.lock().await;

                if let Some(room) = rooms_guard.get(code) {
                    if let Some(target_id) = target_peer_id {
                        if let Some(target_peer) = room.peers.iter().find(|p| p.id == target_id) {
                            let _ = target_peer.tx.send(to_message(&ServerMsg::Relay {
                                peer_id: peer_id.clone(),
                                payload: &payload,
                            }));
                        }
                    } else {
                        for peer in &room.peers {
                            if peer.id != peer_id {
                                let _ = peer.tx.send(to_message(&ServerMsg::Relay {
                                    peer_id: peer_id.clone(),
                                    payload: &payload,
                                }));
                            }
                        }
                    }
                }
            }
        }
    }

    // Limpeza ao desconectar
    if let Some(code) = current_room {
        let mut rooms_guard = rooms.lock().await;
        if let Some(room) = rooms_guard.get_mut(&code) {
            room.peers.retain(|p| p.id != peer_id);
            let count = room.peers.len();

            for peer in &room.peers {
                let _ = peer.tx.send(to_message(&ServerMsg::PeerLeft {
                    peer_id: peer_id.clone(),
                }));
                let _ = peer.tx.send(to_message(&ServerMsg::RoomUpdate { count }));
            }

            if room.peers.is_empty() {
                rooms_guard.remove(&code);
            }
        }
    }

    let _ = send_task.await;
}
