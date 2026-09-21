# Sharecrow Signal

Um servidor de sinalização leve, concorrente e ultrarrápido desenvolvido em **Rust** utilizando o framework **Axum**. Este projeto serve como o backend de sinalização para o aplicativo cliente Sharecrow, sendo responsável por intermediar a troca de mensagens (SDP e ICE Candidates) para o estabelecimento de conexões **WebRTC** P2P.

## Funcionalidades

- **WebSockets:** Comunicação bidirecional e em tempo real.
- **Sistema de Salas (Rooms):** 
  - Geração automática de códigos únicos (6 caracteres, sem ambiguidade).
  - Limite de 2 conexões simultâneas (peers) por sala para sessões 1-a-1.
- **Relay P2P:** Repasse eficiente de payloads JSON entre os pares conectados.
- **Gerenciamento de Estado:** Limpeza automática de salas quando os clientes se desconectam.

## Tecnologias Utilizadas

- [Rust](https://www.rust-lang.org/)
- [Axum](https://github.com/tokio-rs/axum) (Web framework)
- [Tokio](https://tokio.rs/) (Runtime Assíncrono)
- [Serde](https://serde.rs/) (Serialização/Desserialização JSON)
- [Tungstenite](https://github.com/snapview/tungstenite-rs) (WebSockets via Axum)

## Como rodar localmente

Certifique-se de ter o **Cargo** e o **Rust** instalados na sua máquina.

1. Clone o repositório:
   ```bash
   git clone [https://github.com/SEU-USUARIO/sharecrow-signal.git](https://github.com/SEU-USUARIO/sharecrow-signal.git)
   cd sharecrow-signal
