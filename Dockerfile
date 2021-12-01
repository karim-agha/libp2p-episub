FROM nwtgck/rust-musl-builder:latest AS rust-build
ADD . /code
RUN sudo chown -R rust /code
RUN cd /code && cargo build --release --target x86_64-unknown-linux-musl --example p2plab


FROM alpine:latest
WORKDIR /home
COPY --from=rust-build /code/target/x86_64-unknown-linux-musl/release/examples/p2plab .
EXPOSE 4001
CMD ["./p2plab", "--topic", "/topic/zero", "-vv"]
