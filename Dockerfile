FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN git submodule update --init --recursive 2>/dev/null || true

RUN cmake -S . -B build \
      -DCMAKE_BUILD_TYPE=Release \
      -DRAL_BUILD_ADAPTERS=ON \
    && cmake --build build -j$(nproc)

# ---- Runtime image ----
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/adapters/device-twin/device-twin /usr/local/bin/
COPY adapters/device-twin/configs/ /etc/adapters/device-twin/

ENTRYPOINT ["device-twin"]
CMD ["/etc/adapters/device-twin/acct-example.yml"]
