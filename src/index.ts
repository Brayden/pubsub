import { Hono } from "hono";
import { Env } from "./types/env";
import { ClientDurableObject } from "./durable-objects/client";
import { getFirstAvailableTopicShard, TopicDurableObject } from "./durable-objects/topic";
import { MessagePayload } from "./types/mqtt";

export { ClientDurableObject, TopicDurableObject };

// TODOS:
// - How does shard-0 communicate to all its peer shards (e.g. shard-1, shard-2, shard-N)
// - How should we handle durable retries to ensure a client receives the messages (should the client acknowledge receipt?)
// - How do we handle authentication? Auth per load system?
// - How do we handle QoS levels 1+ (keep message in db and retry list of clients until all have acknowledged receipt)
// - How should we handle rate limiting the consumers as to not bottleneck the downstream?
// - How do we handle shard consolidation? If we peak at 5 shards, then connections go down to support 1 shard, how do we reassign?
// - Can we handle FIFO (first-in, first-out) where messages are delivered to subscribers in the same order they were published. Strict message ordering & dedupe.
// - Subscribe with topic prefixing, e.g. `prefix:*` or `prefix:abc`
// - If subscribing to multiple topics via the ClientDO can they subscribe by passing in an array of topics up front
// - Replay events from a specific bookmark (can we store all events and have a bookmarkId that corresponds to a timestamp)
// - Can shards support geographically sharding?

// Completed:
// - A user should be able to connect to ClientDO (multiplex) or TopicDO (direct)


export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        const app = new Hono();

        app.get('/publish', async (c) => {
            // Deliver to the right channel
            // And also deliver to all existing shards of that channel

            const payload: MessagePayload = await c.req.json();
            console.log('Payload: ', payload)
        })

        app.get('/subscribe', async (c) => {
            // When a clientId exists we will opt to use Browser <-> Client <-> Topic multiplexing with the client DO instance.
            const clientId = c.req.query('clientId');
            // The topic is used when an incoming request wants to subscribe directly to the topic DO instance.
            const topic = c.req.query('topic');

            // An id from one of the two availble methods is required.
            if (!clientId && !topic) {
                return c.text('No `clientId` or `topic` provided.')
            }
            
            // Instantiate our user DO to handle establishing our web socket
            // connection between the client (browser) <-> client session (DO).
            if (clientId) {
                const stubId = env.CLIENT_DURABLE_OBJECT.idFromName(clientId);
                const stub = env.CLIENT_DURABLE_OBJECT.get(stubId);
                return stub.fetch(request);
            }

            // Otherwise if `topicId` is set the relationship between the incoming
            // client (e.g. browser, IoT, etc) will have a direct socket connection
            // relationship with the Topic.
            if (topic) {
                const stub = await getFirstAvailableTopicShard(topic, env, 0);

                if (stub === null) {
                    return new Response('No available shard for connections.')
                }

                return stub.fetch(request);
            }
        })

        app.get('/unsubscribe', async (c) => {
            // TODO
        })
        
        return app.fetch(request, env, ctx);
    }
} satisfies ExportedHandler<Env>;
