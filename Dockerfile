FROM scratch
ENV SSL_CERT_FILE=/usr/local/ssl/cacert.pem

COPY target/x86_64-unknown-linux-musl/release/lout /
COPY examples/s3.toml /etc/lout/config.toml
COPY cacert.pem /usr/local/ssl/

ENTRYPOINT ["/lout", "/etc/lout/config.toml"]
