FROM nwtgck/rust-musl-builder:latest AS rust-build
ADD . /code
RUN sudo chown -R rust /code
RUN cd /code && cargo build --release --target x86_64-unknown-linux-musl


FROM alpine:latest
WORKDIR /home
COPY --from=rust-build /code/target/x86_64-unknown-linux-musl/release/episub-node .
COPY --from=rust-build /code/target/x86_64-unknown-linux-musl/release/audit-node .
COPY --from=rust-build /code/audit/assets ./assets

EXPOSE 4001
EXPOSE 80
