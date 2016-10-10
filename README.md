#LOUT

### lout is a Log rOUTer. get it? 

As a router should be, it has configurable inputs, outputs and routes to each.  Right now it supports Gelf v2 protocol as input, and S3, Elasticsearch and Stdout as output.

Configuration uses the TOML format, which is similar to YAML but supports multiple config sections, similar to an ini file. 

An example config would looks something like : 
```
[input]

[input.gelf]
url = "localhost:5555"


[output]

[output.mys3]
batch_secs = 20
type = "s3"
batch_max = 20000
batch_directory = "."
bucket = "dev.service.events"

[output.mystdout]
type = "stdout"

[route]

[route.default]
input = "gelf"
output = "mys3"
if_has_key = "pd_event"

[route.default]
input = "gelf"
output = "mystdout"
if_has_key = "pd_log"

```

Note that gelf listens on UDP, so the correct UDP port would need to be opened.


## Building 

Lout is written in Rust, which, like Go, can compile into a completely static binary to make deployment easier. 
Unlike Go, it is not particularly easy to build using this static target. You need a rustc that supports a musl target. 
Musl is a tiny, statically linked standard library. It is a replacement for glibc.  This is easy enough to get working
with Rust, however, there are other dependencies, such as OpenSSl, which also need to be built statically so they can be
linked in. 

Fortunately someone has gone through the trouble of creating a docker image which has the rustc musl target as well as
necessary library dependencies.  This is :https://github.com/clux/muslrust 
run_in_docker.sh is a wrapper script which lets you run commands in a container as your $USER within docker. 

So compiling should be as easy as :

```
./run_in_docker.sh cargo build --release
```

This will deposit a release binary in : 
./target/x86_64-unknown-linux-musl/release/lout

To build a new docker image with that new binary, run: 

```
docker build -t git.permissiondata.com:4567/devops/lout .
```

It works best when this binary is given net=host access. This is its intended config when running in k8s, give it a try using this command: 

```
docker run -e "AWS_ACCESS_KEY_ID=<Some S3 capable key>" -e "AWS_SECRET_ACCESS_KEY=<secret for said key>" --net=host --privileged=true -v $PWD:/here -w /here git.permissiondata:4567/devops/lout
```
