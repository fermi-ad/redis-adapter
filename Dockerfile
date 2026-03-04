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
    && strip build/adapters/device-twin/device-twin

# ---- Runtime image ----
FROM scratch

COPY --from=builder /src/build/adapters/device-twin/device-twin /device-twin
COPY adapters/device-twin/configs/ /etc/adapters/device-twin/

CMD ["/device-twin", "/etc/adapters/device-twin/acct-example.yml"]
