# ConsenSource REST API

[![Build Status](https://travis-ci.org/target/consensource-api.svg?branch=master)](https://travis-ci.org/target/consensource-api) [![Coverage Status](https://img.shields.io/coveralls/github/target/consensource-api)](https://coveralls.io/github/target/consensource-api?branch=master)

The ConsenSource REST API is a Rust server with endpoints for fetching data
from [the ConsenSource database](https://github.com/target/consensource-database), and posting transactions/batches to [the ConsenSource processor](https://github.com/target/consensource-processor).

## How It Works

### Batches

Batches submitted from a client (web app, cli, etc) are sent to the `/batches` endpoint as a serialized payload.
This endpoint deserializes the payload and creates a [protocol buffer](https://developers.google.com/protocol-buffers).
The ConsenSource protobuf definitions can be found in [the ConsenSource common repo](https://github.com/target/consensource-common/tree/master/protos) repo.

The protobuf is sent to [the ConsenSource processor](https://github.com/target/consensource-processor), and the REST API responds with a link from

`/batch_statuses?<batch_ids>`

This endpoint monitors Sawtooth state and returns a JSON payload indicating the status of a batch, and if has been committed to a block.

### Server-Sent Events (SSE)

A SSE server is created along with the REST API in order to send new data to [the ConsenSource UI](https://github.com/target/consensource-ui).
Details on SSE can be found on [the Mozilla docs](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events).
The Rust library we are using for SSE can be found [here](https://github.com/adeebahmed/hyper-sse/tree/0.1-no-tokens).

### Private Key Storage

Endpoints are provided at `/api/key` to interface with a [HashiCorp Vault](https://github.com/hashicorp/vault) instance for storing and retrieving user private keys. These endpoints will only work with OAuth enabled as they login to a Vault instance through LDAP. A number of extra environment variables are expected, including `VAULT_URL`, `VAULT_PATH`, `VAULT_USERNAME`, and `VAULT_PASSWORD`. These are expected in a top-level `.env` if using docker compose.

These endpoints expect an Authorization Bearer JWT token as a header that includes a field `username` once decoded.

## Development

The ConsenSource REST API is written using the [Rocket web framework](https://rocket.rs/).
It requires nightly, though is very close to being [stabilized](https://github.com/SergioBenitez/Rocket/issues/19).

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

Most of the Rest API tests are integration tests. To start up a Postgres and Rest API instance and run these tests with code coverage metrics:

```
cd test/
docker-compose -f docker-compose.tarpaulin.yaml up
```

#### Writing integration tests

While writing intergration tests, you will need a Postgres instance to issue commands against.
This can be done with the following command:

```
cd test/
docker-compose -f docker-compose.dev.yaml up
```

Then, to run your tests you will need to exec into `test-rest-api` container:

```
docker exec -it test-rest-api /bin/bash
cd /api
```

From there you can run `cargo test -- --nocapture --test-threads=1` to run all tests. It is important to include the `--test-threads=1` to prevent the tests from running in parallel, due to database race conditions.

As you update your code, the shared volume mounted to `/api` will allow you to run tests in the container with your most up-to-date code.

### Build

```
cargo build
```

### Run

Please visit the [main ConsenSource repo](https://github.com/target/consensource) for instructions on how to run with other components, using either `docker-compose` or `kubernetes`.
