FROM rust:latest as builder
WORKDIR /usr/src/app
COPY . .
RUN cargo build --release

# Estágio final de execução
FROM debian:bookworm-slim
WORKDIR /usr/local/bin
COPY --from=builder /usr/src/app/target/release/sharecrow-signal .

ENV PORT=8787
EXPOSE 8787

CMD ["sharecrow-signal"]