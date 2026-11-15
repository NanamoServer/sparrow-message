# Sparrow Message

English | [简体中文](README_CN.md)

Sparrow Message is a lightweight messaging library built for Minecraft server networks. It sits on top of Redis Pub/Sub and gives you typed one-way messages and request/response calls between servers, with a binary wire format you control.

It requires Java 21. Redis 6+ or Valkey works as the backend.

## Features

- One-way messages to every server, every other server, one server, or all servers carrying a tag.
- Request/response between two servers, returned as a `CompletableFuture`, with a default timeout and a per-call override.
- Fast failure: a request to an offline server fails immediately, and a failure in the remote handler is reported back to the caller.
- Messages addressed to the local server are handled in place, without touching Redis.
- Handlers run on the Redis I/O thread by default, in arrival order and with no scheduling overhead. A message that needs to block opts into virtual threads.
- A message can be encoded into a channel name and bytes, to be published from a Lua script atomically with other Redis writes.
- Serialization is yours: each message reads and writes a Netty `ByteBuf` directly.
- Several brokers can share one Redis connection, one per feature of your plugin.
- No dependencies beyond Lettuce and the Netty that ships with your platform.

## Architecture

```mermaid
flowchart LR
    App["Plugin"] --> Broker["MessageBroker"]
    Broker --> Registry["RedisMessageRegistry"]
    Broker --> Header["Header + MessageCodec"]
    Broker --> Conn["RedisConnection"]
    Conn --> All["{appId}:all"]
    Conn --> Server["{appId}:server:{serverId}"]
```

```text
src/main/java/net/momirealms/sparrow/message
+-- MessageBroker            Main API and its Builder
+-- RedisMessage             Base interface of every message
+-- MessageIdentifier        Name of a message type
+-- Target                   Where a one-way message goes
+-- PreparedMessage          An encoded one-way message, for publishing from a Lua script
+-- Logger                   Log sink you implement
+-- message                  OneWayMessage, TwoWayRequestMessage, TwoWayResponseMessage
+-- codec                    MessageCodec and its encoder / decoder halves
+-- registry                 Message registration and wire id assignment
+-- connection               RedisConnection and the Lettuce implementation
+-- executors                Direct, virtual-thread and serial executors
+-- exception                NoSubscriberException, RemoteMessageException
+-- util                     ByteBufHelper
```

A `MessageBroker` is identified by an `appId` and a `serverId`. Brokers with the same `appId` talk to each other, and every broker subscribes to two channels:

| Channel | Carries |
|---|---|
| `{appId}:all` | Broadcasts and tag-targeted messages |
| `{appId}:server:{serverId}` | Messages, requests and responses addressed to that server |

Every payload starts with a small header written by the library, followed by the body written by the message's codec:

```text
byte    flags            bits 0-1: kind (one-way, request, response, error), bit 2: has tag, bit 3: exclude source
int     type id          hash of the message identifier by default, fixed 4 bytes; absent on error responses
utf8    tag              only when bit 2 is set
utf8    source server    only when bit 3 is set or the kind is request
long    correlation id   absent on one-way messages
...     body
```

A plain broadcast costs five bytes of header. The broker can be built to assign type ids by registration order instead, which makes the type id a varint and brings a plain broadcast down to two bytes, see [Things to know](#things-to-know). Tag and source filtering happen right after the header is read, so a server that is not a recipient never decodes the body.

## Getting started

The library declares Lettuce as `compileOnly`, so you provide it yourself. Netty comes from your platform (Paper, Velocity and BungeeCord all ship it) or transitively from Lettuce. Relocate `net.momirealms.sparrow.message` when you shade it into a plugin.

### Create a broker

```java
RedisClient client = RedisClient.create("redis://127.0.0.1:6379");
RedisConnection connection = new LettuceRedisConnection(client);

MessageBroker broker = MessageBroker.builder()
        .appId("myplugin:chat")
        .serverId("survival-1")
        .tags(Set.of("survival"))
        .connection(connection)
        .logger(myLogger)
        .requestTimeout(Duration.ofSeconds(10))   // optional, 10 seconds by default
        .typeIdStrategy(TypeIdStrategy.IDENTIFIER_HASH)   // optional, IDENTIFIER_HASH by default
        .build();
```

`appId` is used verbatim as the channel prefix. Redis channels are global to the whole instance, regardless of the selected database, so write it as `plugin:feature` and add an environment segment if several networks share one Redis.

Register every message, then subscribe:

```java
broker.registry().register(HelloMessage.ID, HelloMessage.CODEC);
broker.registry().register(PlayerCountRequest.ID, PlayerCountRequest.CODEC);
broker.registry().register(PlayerCountResponse.ID, PlayerCountResponse.CODEC);
broker.subscribe();
```

On shutdown:

```java
broker.unsubscribe();
connection.close();
client.shutdown();
```

The connection belongs to whoever created it. Several brokers may share one `RedisConnection`; closing it is your job, not the broker's.

### Define a one-way message

```java
public final class HelloMessage extends OneWayMessage {
    public static final MessageIdentifier ID = MessageIdentifier.of("myplugin", "hello");
    public static final MessageCodec<HelloMessage> CODEC = MessageCodec.of(HelloMessage::write, HelloMessage::new);

    private final String text;

    public HelloMessage(String text) {
        this.text = text;
    }

    private HelloMessage(ByteBuf buffer) {
        this.text = ByteBufHelper.readUtf8(buffer);
    }

    private void write(ByteBuf buffer) {
        ByteBufHelper.writeUtf8(buffer, this.text);
    }

    @NotNull
    @Override
    public MessageIdentifier identifier() {
        return ID;
    }

    @Override
    public void handle() {
        System.out.println("Received: " + this.text);
    }
}
```

`handle()` runs on the receiving server. The constructor that takes a `ByteBuf` and the `write` method only deal with the message's own fields; routing information lives in the header.

To read and write through another buffer type, such as Minecraft's `FriendlyByteBuf`, pass a wrapper:

```java
MessageCodec<HelloMessage> CODEC = MessageCodec.of(FriendlyByteBuf::new, HelloMessage::write, HelloMessage::new);
```

### Send one-way messages

```java
broker.publishAll(new HelloMessage("to everyone"));                           // every server, including this one
broker.publishOthers(new HelloMessage("to everyone else"));                   // every server except this one
broker.publish(new HelloMessage("to one server"), "survival-2");              // one server
broker.publish(new HelloMessage("to tagged servers"), Target.tag("survival")); // servers carrying the tag
```

Each call returns a `CompletableFuture<Long>` that completes once Redis has accepted the message. The value is the number of subscriber connections that received it. Zero is still a success, and the future does not wait for the remote handler.

```java
broker.publish(new HelloMessage("hi"), "survival-2")
        .thenAccept(receivers -> System.out.println("delivered to " + receivers + " subscriber(s)"));
```

### Request and response

A request names its response type and produces it in `handleRequest()`:

```java
public final class PlayerCountRequest extends TwoWayRequestMessage<PlayerCountResponse> {
    public static final MessageIdentifier ID = MessageIdentifier.of("myplugin", "player_count_request");
    public static final MessageCodec<PlayerCountRequest> CODEC = MessageCodec.of(PlayerCountRequest::write, PlayerCountRequest::new);

    private final String world;

    public PlayerCountRequest(String world) {
        this.world = world;
    }

    private PlayerCountRequest(ByteBuf buffer) {
        this.world = ByteBufHelper.readUtf8(buffer);
    }

    private void write(ByteBuf buffer) {
        ByteBufHelper.writeUtf8(buffer, this.world);
    }

    @NotNull
    @Override
    public MessageIdentifier identifier() {
        return ID;
    }

    @NotNull
    @Override
    public CompletableFuture<PlayerCountResponse> handleRequest() {
        // this.sourceServer() is the server that sent the request
        return CompletableFuture.completedFuture(new PlayerCountResponse(42));
    }
}

public final class PlayerCountResponse extends TwoWayResponseMessage {
    public static final MessageIdentifier ID = MessageIdentifier.of("myplugin", "player_count_response");
    public static final MessageCodec<PlayerCountResponse> CODEC = MessageCodec.of(PlayerCountResponse::write, PlayerCountResponse::new);

    private final int count;

    public PlayerCountResponse(int count) {
        this.count = count;
    }

    private PlayerCountResponse(ByteBuf buffer) {
        this.count = ByteBufHelper.readCompactInt(buffer);
    }

    private void write(ByteBuf buffer) {
        ByteBufHelper.writeCompactInt(buffer, this.count);
    }

    public int count() {
        return this.count;
    }

    @NotNull
    @Override
    public MessageIdentifier identifier() {
        return ID;
    }
}
```

The future returned by `handleRequest()` may complete later and on any thread; the broker sends the response when it does.

```java
broker.publishTwoWay(new PlayerCountRequest("world"), "survival-2")
        .thenAccept(response -> System.out.println("players: " + response.count()));
```

Pass a timeout for calls that need more or less time than the default. It is measured from the moment of the call:

```java
broker.publishTwoWay(new PlayerCountRequest("world"), "survival-2", Duration.ofSeconds(30))
        .whenComplete((response, error) -> {
            if (error == null) {
                System.out.println("players: " + response.count());
                return;
            }
            Throwable cause = error instanceof CompletionException ? error.getCause() : error;
            if (cause instanceof NoSubscriberException) {
                System.out.println("target is offline");
            } else if (cause instanceof TimeoutException) {
                System.out.println("no response in time");
            } else if (cause instanceof RemoteMessageException remote) {
                System.out.println("target failed: " + remote.exceptionClass() + " " + remote.exceptionMessage());
            }
        });
```

| The future fails with | When |
|---|---|
| `NoSubscriberException` | The target server is not subscribed, usually because it is offline. Fails at once. |
| `java.util.concurrent.TimeoutException` | No response arrived in time. The remote side is not cancelled and a late response is ignored. |
| `RemoteMessageException` | `handleRequest()` threw or its future failed. Carries the exception class name and message; the full stack trace is logged on the handling server. |
| `IllegalStateException` | The broker unsubscribed while waiting. |

A `handleRequest()` future that completes with `null` sends no response, and the caller runs into its timeout.

### Threads and ordering

Handlers run on the executor returned by the message's `executor()`.

| Executor | Runs the handler on | Use it when |
|---|---|---|
| `MessageExecutors.SYNC` (default) | The Redis I/O thread, one message after another in arrival order | The handler is quick and never blocks, which covers most messages |
| `MessageExecutors.VIRTUAL` | A fresh virtual thread per message, with no ordering | The handler blocks: a database query, a synchronous Redis call, waiting on a future |
| `MessageExecutors.newSerialExecutor()` | Virtual threads, one task at a time in submission order | The handler blocks and order still matters |

**A handler on the default executor must not block and must not call Redis synchronously.** Every broker sharing the connection receives its messages on that one thread, so a handler that blocks holds up all of them. Keep message bodies small for the same reason: decoding happens on that thread too.

Futures follow the same split:

- A one-way publish future completes on the Redis I/O thread. Callbacks attached to it must not block; use `thenAcceptAsync` and friends when they need to.
- A request future completes on a virtual thread, so code that runs once the response arrives may block.

To handle a blocking message in arrival order, return the same serial executor from every message in the group:

```java
private static final Executor ORDERED = MessageExecutors.newSerialExecutor();

@NotNull
@Override
public Executor executor() {
    return ORDERED;
}
```

A serial executor runs one task at a time, in submission order. Ordering covers the handler up to the point it returns; work it hands off elsewhere is yours to coordinate. Use a separate serial executor for each group of messages, so one slow group does not hold up another.

### Local delivery

A message or request addressed to the local `serverId` skips Redis and serialization entirely: the handler receives the very object you passed in, on the message's executor. With the default executor that means the handler runs on the thread that called `publish` or `publishTwoWay`, before the call returns. Timeouts and failure rules stay the same.

Broadcasts and tag targets that include the local server still travel through Redis, so every server observes them in the same order.

To send a server-addressed message through Redis even when the target is the local `serverId`, for example to reach another process that uses the same id, override:

```java
@Override
public boolean shortCircuitLocally() {
    return false;
}
```

### Publishing from a Lua script

When a Redis write and a message must happen atomically, `prepare` gives you the channel name and the encoded bytes, and your own script does the `PUBLISH`:

```java
private static final String UPDATE = "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]); return redis.call('PUBLISH', ARGV[3], ARGV[4])";

PreparedMessage prepared = broker.prepare(new PlayerChangedMessage(uuid), Target.all());
commands.eval(
        UPDATE,
        ScriptOutputType.INTEGER,
        new byte[][]{KEY},
        field,
        value,
        prepared.channel().getBytes(StandardCharsets.UTF_8),
        prepared.payload()
);
```

The script runs on your own Redis connection; the library is not involved. `prepare` accepts every `Target` and works for one-way messages only. It never short-circuits locally, so a message addressed to the local server still travels through Redis.

## Things to know

- **Type ids come from the message identifier by default.** With `TypeIdStrategy.IDENTIFIER_HASH` the type id is the 32-bit FNV-1a hash of `namespace:value`. It does not depend on registration order, and servers may register different sets of messages. When two identifiers hash to the same value, registration throws `IllegalStateException`; rename one of them. Changing the fields of a message still requires updating all servers together, or giving the new format a new identifier.
- **With `TypeIdStrategy.REGISTRATION_ORDER`, registration order is the wire format.** The type id is the position in registration order, which saves 3 bytes per message. Every broker with the same `appId` must register the same messages in the same order, before subscribing, and adding a message in the middle requires updating all servers together. The library does not check that the order matches.
- **Every broker with the same `appId` must use the same `TypeIdStrategy`.** The two header formats are incompatible and the library does not check that servers agree; a mismatch shows up as dropped messages or decode failures.
- **A message must be registered before it is published**, and so must the response type of a request.
- **Delivery is at most once.** Redis Pub/Sub does not store messages. A server that is offline or disconnected misses whatever is published meanwhile.
- **`publishOthers` counts the local subscription too.** Redis still delivers the message to the sending server, which then discards it.
- **Handler failures go to your `Logger`.** An exception thrown by `handle()`, a payload that fails to decode, and a failed `handleRequest()` are all reported through `Logger.error`.
- **Messages with an unknown type id are dropped silently.**


## Dependency

```kotlin
repositories {
    mavenCentral()
    maven("https://repo.momirealms.net/releases/")
}

dependencies {
    implementation("net.momirealms:sparrow-message:26.11.1")
    implementation("io.lettuce:lettuce-core:7.7.0.RELEASE")
}
```