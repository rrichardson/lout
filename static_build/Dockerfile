FROM scratch
ENV SSL_CERT_FILE=/usr/local/ssl/cacert.pem

COPY target/x86_64-unknown-linux-musl/release/lout /
COPY cacert.pem /usr/local/ssl/

COPY pachctl /opt/pachyderm/bin/

#for testing
#COPY examples/s3.toml /etc/lout/config.toml

ENTRYPOINT ["/lout", "/etc/lout/config.toml"]
