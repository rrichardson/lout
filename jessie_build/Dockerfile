FROM git.permissiondata.com:4567/devops/pachyderm:fedora
MAINTAINER "Rick Richardson <rrichardson@permissiondata.com>"

COPY target/debug/lout /

CMD ["/lout", "/etc/lout/config.toml"]
