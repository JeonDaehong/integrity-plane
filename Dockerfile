# integrity-server and the integrity CLI (spec §27 DoD 1; deploy/docker-compose.yml).
# Reproducible: locked dependencies, base images pinned by digest, fixed build path; CI builds it
# twice and compares the binaries (release-hygiene.yml). Bump the digests deliberately.
FROM rust:1.90-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f AS build
WORKDIR /src
ENV CARGO_INCREMENTAL=0
COPY . .
RUN cargo build --release --locked -p integrity-server -p integrity-cli

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/integrity-server /src/target/release/integrity /usr/local/bin/
EXPOSE 8181
VOLUME /var/lib/integrity
ENTRYPOINT ["integrity-server"]
CMD ["/etc/integrity/integrity.toml"]
