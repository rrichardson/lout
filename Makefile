
TAG = 0.3

all: push

target/release/lout:
	cargo build --release

target/x86_64-unknown-linux-musl/release/lout:
	./run_in_docker.sh cargo build --release

build: target/release/lout
	sudo docker build -t git.permissiondata.com:4567/devops/lout:$(TAG) .

build_static: target/x86_64-unknown-linux-musl/release/lout:
	cd static_build
	sudo docker build -t git.permissiondata.com:4567/devops/lout:$(TAG) .

push: build
	sudo docker push git.permissiondata.com:4567/devops/lout:$(TAG)