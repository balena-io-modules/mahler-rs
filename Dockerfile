ARG ALPINE_VERSION=3.20

FROM alpine:${ALPINE_VERSION}

ARG RUST_VERSION=1.82

RUN apk add --update --no-cache \
	build-base \
	clang-dev \
	cmake \
	rustup

#	Ensure binaries installed with `cargo install`
# can be found
ENV PATH="${PATH}:/root/.cargo/bin"

RUN rustup-init --default-toolchain=$RUST_VERSION -y

WORKDIR /usr/src/app

COPY . ./

CMD "./examples/run-tests.sh"
