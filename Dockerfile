FROM nwtgck/rust-musl-builder:latest AS rust-build
ADD . /code
RUN sudo chown -R rust /code
RUN cd /code && cargo build --release --target x86_64-unknown-linux-musl -p episub-node


FROM alpine:latest
WORKDIR /home
COPY --from=rust-build /code/target/x86_64-unknown-linux-musl/release/episub-node .
EXPOSE 4001
CMD ["./episub-node", "--topic", "/topic/zero", "-vv"]
