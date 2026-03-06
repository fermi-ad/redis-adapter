FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN git submodule update --init --recursive 2>/dev/null || true

RUN cmake -S . -B build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_C_FLAGS="-O2" \
      -DCMAKE_CXX_FLAGS="-O2" \
      -DCMAKE_EXE_LINKER_FLAGS="-static" \
      -DRAL_BUILD_ADAPTERS=ON \
    && cmake --build build -j$(nproc) \
    && strip build/adapters/device-twin/device-twin \
    && strip build/adapters/baseline-subtract/baseline-subtract \
    && strip build/adapters/integrate/integrate \
    && strip build/adapters/filter/filter \
    && strip build/adapters/magnitude/magnitude \
    && strip build/adapters/position-intensity/position-intensity \
    && strip build/adapters/demux/demux \
    && strip build/adapters/fft/fft \
    && strip build/adapters/bpm/bpm \
    && strip build/adapters/bpm-twin/bpm-twin \
    && strip build/adapters/blm/blm \
    && strip build/adapters/blm-twin/blm-twin \
    && strip build/adapters/bcm/bcm \
    && strip build/adapters/bcm-twin/bcm-twin

# ---- Redis TUI (Rust) ----
FROM rust:latest AS tui-builder

RUN apt-get update && apt-get install -y musl-tools && rm -rf /var/lib/apt/lists/*
RUN MUSL_TARGET="$(uname -m)-unknown-linux-musl" && rustup target add "$MUSL_TARGET"

WORKDIR /app
RUN MUSL_TARGET="$(uname -m)-unknown-linux-musl" \
    && git clone --depth 1 https://github.com/bigsamich/redis-tui.git . \
    && cargo build --release --target "$MUSL_TARGET" \
    && strip target/$MUSL_TARGET/release/redis-tui \
    && ln -s $MUSL_TARGET target/musl-release

# ---- Instrument TUI (Rust) ----
FROM rust:latest AS inst-tui-builder

RUN apt-get update && apt-get install -y musl-tools && rm -rf /var/lib/apt/lists/*
RUN MUSL_TARGET="$(uname -m)-unknown-linux-musl" && rustup target add "$MUSL_TARGET"

WORKDIR /app
COPY adapters/inst-tui/ .
RUN MUSL_TARGET="$(uname -m)-unknown-linux-musl" \
    && cargo build --release --target "$MUSL_TARGET" \
    && strip target/$MUSL_TARGET/release/inst-tui \
    && ln -s $MUSL_TARGET target/musl-release

# ---- Runtime image ----
FROM scratch

COPY --from=builder /src/build/adapters/device-twin/device-twin /device-twin
COPY --from=builder /src/build/adapters/baseline-subtract/baseline-subtract /baseline-subtract
COPY --from=builder /src/build/adapters/integrate/integrate /integrate
COPY --from=builder /src/build/adapters/filter/filter /filter
COPY --from=builder /src/build/adapters/magnitude/magnitude /magnitude
COPY --from=builder /src/build/adapters/position-intensity/position-intensity /position-intensity
COPY --from=builder /src/build/adapters/demux/demux /demux
COPY --from=builder /src/build/adapters/fft/fft /fft
COPY --from=builder /src/build/adapters/bpm/bpm /bpm
COPY --from=builder /src/build/adapters/bpm-twin/bpm-twin /bpm-twin
COPY --from=builder /src/build/adapters/blm/blm /blm
COPY --from=builder /src/build/adapters/blm-twin/blm-twin /blm-twin
COPY --from=builder /src/build/adapters/bcm/bcm /bcm
COPY --from=builder /src/build/adapters/bcm-twin/bcm-twin /bcm-twin
COPY --from=tui-builder /app/target/musl-release/release/redis-tui /redis-tui
COPY --from=inst-tui-builder /app/target/musl-release/release/inst-tui /inst-tui

COPY adapters/device-twin/configs/ /etc/adapters/device-twin/
COPY adapters/baseline-subtract/configs/ /etc/adapters/baseline-subtract/
COPY adapters/integrate/configs/ /etc/adapters/integrate/
COPY adapters/filter/configs/ /etc/adapters/filter/
COPY adapters/magnitude/configs/ /etc/adapters/magnitude/
COPY adapters/position-intensity/configs/ /etc/adapters/position-intensity/
COPY adapters/demux/configs/ /etc/adapters/demux/
COPY adapters/fft/configs/ /etc/adapters/fft/
COPY adapters/bpm/configs/ /etc/adapters/bpm/
COPY adapters/bpm-twin/configs/ /etc/adapters/bpm-twin/
COPY adapters/blm/configs/ /etc/adapters/blm/
COPY adapters/blm-twin/configs/ /etc/adapters/blm-twin/
COPY adapters/bcm/configs/ /etc/adapters/bcm/
COPY adapters/bcm-twin/configs/ /etc/adapters/bcm-twin/

CMD ["/device-twin", "/etc/adapters/device-twin/acct-example.yml"]
