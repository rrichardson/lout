FROM git.permissiondata.com:4567/devops/pachyderm:jessie
MAINTAINER "Rick Richardson <rrichardson@permissiondata.com>"

ADD target/release/lout /

ENTRYPOINT ["/lout", "/etc/lout/config.toml"]
