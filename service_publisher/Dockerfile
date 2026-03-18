FROM erlang:26.2.5.18 AS builder

RUN apt-get update && apt-get install -y cmake

WORKDIR /app

# 1. Copy config files first to cache dependencies
COPY rebar.config rebar.lock ./
RUN rebar3 get-deps

# 2. Copy source and build the release
COPY . .
RUN rebar3 compile
RUN rebar3 as prod release

# Stage 2: Runtime
FROM erlang:26.2.5.18-slim

WORKDIR /app

# Copy the release from the builder stage
COPY --from=builder /app/_build/prod/rel/service_publisher .

# Set the entrypoint to run in the foreground
ENTRYPOINT ["/app/bin/service_publisher"]
CMD ["foreground"]
