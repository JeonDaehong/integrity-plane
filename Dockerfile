# integrity-server and the integrity CLI (spec §27 DoD 1; deploy/docker-compose.yml).
# Builds from the locked dependency set; base images are pinned by tag.
FROM rust:1.90-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p integrity-server -p integrity-cli

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/integrity-server /src/target/release/integrity /usr/local/bin/
EXPOSE 8181
VOLUME /var/lib/integrity
ENTRYPOINT ["integrity-server"]
CMD ["/etc/integrity/integrity.toml"]
