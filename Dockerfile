# integrity-server and the integrity CLI (spec §27 DoD 1; deploy/docker-compose.yml).
# Reproducible: locked dependencies, base images pinned by digest, fixed build path; CI builds it
# twice and compares the binaries (release-hygiene.yml). Bump the digests deliberately.
FROM rust:1.98-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS build
WORKDIR /src
ENV CARGO_INCREMENTAL=0
COPY . .
RUN cargo build --release --locked -p integrity-server -p integrity-cli

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
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
