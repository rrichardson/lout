.PHONY: clean push push_static

TAG=pg1

all: push

clean:
	rm target/x86_64-unknown-linux-musl/release/lout
	rm target/release/lout

target/release/lout: src/*.rs
	cargo build --release

target/x86_64-unknown-linux-musl/release/lout: src/*.rs
	./run_in_docker.sh cargo build --release

build: target/release/lout
	rm Dockerfile && ln -s jessie_build/Dockerfile .
	sudo docker build -t git.permissiondata.com:4567/devops/lout:$(TAG) .

build_static: target/x86_64-unknown-linux-musl/release/lout
	rm Dockerfile && ln -s static_build/Dockerfile .
	cd static_build
	sudo docker build -t git.permissiondata.com:4567/devops/lout .

push_static:
	sudo docker tag git.permissiondata.com:4567/devops/lout:latest git.permissiondata.com:4567/devops/lout:$(TAG)
	sudo docker push git.permissiondata.com:4567/devops/lout:$(TAG)

push:
	sudo docker push git.permissiondata.com:4567/devops/lout:$(TAG)
