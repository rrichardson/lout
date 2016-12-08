FROM debian:jessie
MAINTAINER "Rick Richardson <rrichardson@permissiondata.com>"

LABEL pachctl.version="1.2.6"
ENV pachctl_version 1.2.6
RUN apt-get -y update &&\
   apt-get -y upgrade &&\
   apt-get -y install wget &&\
   wget https://github.com/pachyderm/pachyderm/releases/download/v${pachctl_version}/pachctl_${pachctl_version}_amd64.deb &&\
   dpkg -i pachctl_${pachctl_version}_amd64.deb &&\
   rm pachctl_${pachctl_version}_amd64.deb &&\
   apt-get clean &&\
   rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

CMD ["/bin/bash"]
