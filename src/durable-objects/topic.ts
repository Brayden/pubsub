import { Hono } from "hono";
import { DurableObject } from "cloudflare:workers";
import { Browsable } from "@outerbase/browsable-durable-object";
import { Env } from "../types/env";

interface TopicMetadata {
    topic: string;
}

const CONNECTIONS_LIMIT = 10_000    // Maximum sockets connection limit per shard
const MAX_SHARD_COUNT = 5           // Maximum number of shards that can exist per topic

// TODO:
// We should probably support a cache map of shards somewhere instead of looping
// through all possible shards to see which one is available. The downside is it
// would require yet another resource to manage. With this approach at least it's
// fully contained (doens't require KV for example) but the downside would be that
// finding a shard to connect to might take slightly longer to loop over potentially
// `MAX_SHARD_COUNT` number of shards.
export async function getFirstAvailableTopicShard(topic: string, env: Env, shardVersion: number = 0): Promise<DurableObjectStub | null> {
    // Create a stub to another Durable Object based on the `topic` value
    // the user wants to subscribe to. In this example any number of users may
    // subscribe to any topic they want, think of it like a public chatroom.
    // Durable Objects can support over 32,000 web socket connections at a time
    // but the more limiting factor is usually the memory resources running the
    // DO's and not the web socket count.
    const stubId = env.TOPIC_DURABLE_OBJECT.idFromName(`${topic}_${shardVersion}`);
    const stub = env.TOPIC_DURABLE_OBJECT.get(stubId);

    // First do an RPC check to see if the shard is overloaded and if so we will
    // check the next shard continuously until one is available.
    const allowed: { success: boolean, limitReached?: boolean } = await stub.canSupportConnection();

    if (!allowed.success) {
        if (allowed.limitReached) {
            throw new Error("Maximum connections to this topic and all shards.");
        }
        
        // If this shard is full but we haven't reached the limit, try the next shard
        if (shardVersion < MAX_SHARD_COUNT) {
            return getFirstAvailableTopicShard(topic, env, shardVersion + 1);
        } else {
            throw new Error("All shards are full for this topic.");
        }
    }

    // Now that we know we have an available shard to us, let's instantiate it
    await stub.init(shardVersion, { topic: topic });
    return stub;
}

@Browsable()
export class TopicDurableObject extends DurableObject<Env> {
    // Hono application instance for serving routes
    private app: Hono = new Hono();
    // Map of connections between the USER <-> CHANNEL (this) durable objects. These do not hibernate.
    private connections = new Map<string, WebSocket>();
    // Which shard version of this channel are we using currently
    private shardVersion: number = 0;
    // Marks which channel this object represents
    private topic: string | undefined = undefined;

    constructor(ctx: DurableObjectState, env: Env) {
        super(ctx, env);
    }

    async init(version: number, metadata: TopicMetadata): Promise<boolean> {
        const { topic } = metadata;
        this.shardVersion = version;

        // TODO: This `this.topic` gets reset when the DO goes to sleep. We need to keep it in storage (e.g. SQLite metadata table).
        this.topic = topic;

        return true;
    }

    async webSocketMessage(ws: WebSocket, message: any) {
        // When a message is received by a USER, just echo back to the USER
        // a confirmation message noting that this particularly channel has
        // received that message.
        ws.send(`[CHANNEL - ${this.topic}-${this.shardVersion}]: Received message from [USER]: ${message}`); //<-- Can we remove this?

        // TESTING: Send all subscribers of a topic the message
        for (const [_, userSocket] of this.connections.entries()) {
            userSocket.send(message);
        }
    }

    async webSocketClose(
        ws: WebSocket,
        code: number,
        reason: string,
        wasClean: boolean
    ) {
        // When a particular user has ended its websocket connection, we should 
        // find their entry in our connections map and prune it from our list we are
        // managing.
        for (const [id, socket] of this.connections.entries()) {
            if (socket === ws) {
                this.connections.delete(id);
                break;
            }
        }

        ws.close(code, "Durable Object is closing WebSocket");
    }

    async fetch(request: Request): Promise<Response> {
        const url = new URL(request.url)

        // Establish a websocket connection between USER <-> CHANNEL durable object (this).
        // While the incoming nature of this websocket _is_ hibernatable (see: `acceptWebSocket`)
        // rather than just `accept`), the outgoing aspect of the USER durable object makes
        // its socket non-hibernatable.
        if (url.pathname === '/subscribe') {
            const webSocketPair = new WebSocketPair();
            const [client, server] = Object.values(webSocketPair);

            // Assign a random identifier to the socket and save the pair locally.
            const connectionId = crypto.randomUUID();
            this.connections.set(connectionId, server);
            this.ctx.acceptWebSocket(server);

            return new Response(null, {
                status: 101,
                webSocket: client,
            });
        }

        return this.app.fetch(request);
    }

    public async canSupportConnection(): Promise<{ success: boolean, limitReached?: boolean }> {
        if (this.connections.size < CONNECTIONS_LIMIT) {
            return { success: true }
        } else if (this.connections.size >= CONNECTIONS_LIMIT && this.shardVersion < MAX_SHARD_COUNT) {
            return { success: false }
        }

        return { 
            success: false,
            limitReached: (this.shardVersion + 1) === MAX_SHARD_COUNT
        }
    }
} 