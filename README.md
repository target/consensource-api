# ConsenSource rest api [![Build Status](https://travis-ci.org/target/consensource-api.svg?branch=master)](https://travis-ci.org/target/consensource-api) [![Coverage Status](https://coveralls.io/repos/github/target/consensource-api/badge.svg?branch=master)](https://coveralls.io/github/target/consensource-api?branch=master)

The ConsenSource rest api is a Rust server that has endpoints for fetching data
from Postgres, and posting transactions/batches to the Processor.

## How the rest api works

### Batches

Batches submitted from a client (web app, cli, etc) are sent to the `/batches`
endpoint as a serialized payload. This endpoint deserializes the payload and
creates a [protocol buffer](https://developers.google.com/protocol-buffers). The
ConsenSource proto formats can be found in the
[_common_](https://github.com/target/consensource-common/tree/master/protos)
repo.

The protobuf is sent to the [Transaction
Processor](https://github.com/target/consensource-processor), and the REST API
responds with a url `/batch_statuses?<batch_ids>`. This endpoint monitors
Sawtooth state and returns a JSON payload indicating the status of a batch, and
if has been committed to a block.

### Server-Sent Events (SSE)

A SSE server is created along with the REST API in order to send new data to
[web ui](https://github.com/target/consensource-ui). Details on SSE can be found
on
[Mozilla's](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events)
docs. The Rust library we are using for SSE can be found
[here](https://github.com/adeebahmed/hyper-sse/tree/0.1-no-tokens).

## Development

The ConsenSource REST API is written using the [Rocket web
framework](https://rocket.rs/). It requires nightly, though is very close to
being [stabilized](https://github.com/SergioBenitez/Rocket/issues/19).

### Switch over to nightly 
``` 
rustup toolchain install nightly rustup default nightly 
```

### Install the nightly linter 

``` 
rustup component add rustfmt --toolchain nightly 
```

### Format (linting) 
``` 
cargo +nightly fmt -- --check 
```

### Test 
``` 
cargo test 
```

### Build 
``` 
cargo build 
```

### Run

You'll need to run ConsenSource from the [compose
repo](https://github.com/target/consensource-compose). The compose repo is a git
submodules repo that  references all the components that make up ConsenSource.

_NOTE: The consensource-compose repo is only for pulling changes and running the
project as a whole, it is not for development._