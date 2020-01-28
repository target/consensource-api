FROM target/consensource-rust:nightly

RUN apt-get update && apt-get install -y unzip libzmq3-dev protobuf-compiler wget

COPY . /api
WORKDIR api
RUN cargo build

ENV PATH=$PATH:/api/target/debug/
