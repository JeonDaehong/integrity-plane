# integrity-server and the integrity CLI (spec §27 DoD 1; deploy/docker-compose.yml).
# Reproducible: locked dependencies, base images pinned by digest, fixed build path; CI builds it
# twice and compares the binaries (release-hygiene.yml). Bump the digests deliberately.
FROM rust:1.99-bookworm@sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c AS build
WORKDIR /src
ENV CARGO_INCREMENTAL=0
COPY . .
RUN cargo build --release --locked -p integrity-server -p integrity-cli

FROM debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /var/lib/integrity integrity \
    && mkdir -p /var/lib/integrity && chown integrity:integrity /var/lib/integrity
COPY --from=build /src/target/release/integrity-server /src/target/release/integrity /usr/local/bin/
# Not root: the control store is the only place the Plane writes.
USER integrity
EXPOSE 8181
VOLUME /var/lib/integrity
ENTRYPOINT ["integrity-server"]
CMD ["/etc/integrity/integrity.toml"]
