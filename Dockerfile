# Estágio de compilação
FROM rust:1.80-slim as builder
WORKDIR /usr/src/app
COPY . .
RUN cargo build --release

# Estágio final de execução
FROM debian:bookworm-slim
WORKDIR /usr/local/bin
COPY --from=builder /usr/src/app/target/release/sharecrow-signal .

# O Render injeta a variável PORT automaticamente
ENV PORT=8787
EXPOSE 8787

CMD ["sharecrow-signal"]