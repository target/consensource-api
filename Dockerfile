FROM target/consensource-rust:nightly

RUN apt-get update && apt-get install -y unzip libzmq3-dev protobuf-compiler wget

COPY . /rest_api
WORKDIR rest_api
RUN cargo build

ENV PATH=$PATH:/rest_api/target/debug/
