FROM debian:jessie

ADD target/release/lout /

ENTRYPOINT ["/lout", "/etc/lout/config.toml"]
