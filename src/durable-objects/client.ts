import { Hono } from "hono";
import { DurableObject } from "cloudflare:workers";
import { Browsable } from "@outerbase/browsable-durable-object";
import { Env } from "../types/env";

@Browsable()
export class ClientDurableObject extends DurableObject<Env> {
    // Hono application instance for serving routes
    private app: Hono = new Hono();
    // Connection between the users clients (e.g. browsers, could be multiple) and the Client singular Durable Object instance (this)
    private connections = new Map<string, WebSocket>();
    // Topics we are subscribed to as a client
    public topics: string[];
    // A map of topic objects we have open web socket communications with
    private topicConnections = new Map<string, WebSocket>();

    constructor(ctx: DurableObjectState, env: Env) {
        super(ctx, env);

        // TODO: We need to seed migrations from a remote source
        // to populate our list of available "topics" in the SQLite
        // data storage layer.
        // The goal of this is to make sure a user cannot subscribe
        // to a topic that would never exist (therefore DO's would never
        // be created for these unused topics).
        // Perhaps we don't seed this at all... or perhasp a user can
        // provide an array of topics hardcoded, or variables in the wrangler
        // or a JSON URL.. we have options.
        this.topics = ['cars', 'bikes', 'trucks', 'boats']
    }

    async webSocketMessage(ws: WebSocket, message: any) {
        // When a message comes in from one of our clients, pass it along to all
        // of the connected topic objects, but also echo back to the client to let
        // them know we received their message.
        for (const [_, socket] of this.connections.entries()) {
            if (socket === ws) {
                // To show the user that we have received their message, immediately
                // echo their message back to them for confirmation of receipt.
                // ws.send(`[USER]: Echo message = ${message}`); // <-- Can we remove this??

                // An incoming message will have two properties to it. The "topic"
                // to mark its intended destination in the multiplexing setup and the
                // "message" to send it.
                const { topic, message: userMessage } = JSON.parse(message)

                // Only forward the users message to the specified topic if the
                // connection to said topic exists.
                const topicSocket = this.topicConnections.get(topic);
                if (topicSocket) {
                    topicSocket.send(userMessage);
                }

                break;
            }
        }

        // If the message is coming from a topic then pass it back to all of our
        // users connected clients letting this USER object act as an intermediary
        // between client <-> topic.
        for (const [_, socket] of this.topicConnections.entries()) {
            if (socket === ws) {
                // Forward message to all user clients
                for (const [_, userSocket] of this.connections.entries()) {
                    userSocket.send(message);
                }

                break;
            }
        }
    }

    async webSocketClose(
        ws: WebSocket,
        code: number,
        reason: string,
        wasClean: boolean
    ) {
        // When a particular client has ended its websocket connection, we should 
        // find its entry in our connections map and prune it from our list we are
        // managing.
        for (const [id, socket] of this.connections.entries()) {
            if (socket === ws) {
                this.connections.delete(id);
                break;
            }
        }

        // When our websocket connections between CLIENT <-> USER durable object
        // have all been closed, we should close all of our non-hibernatable connections
        // between USER <-> topic as well.
        if (this.connections.size === 0) {
            this.changeSubscriptionStatus(false);
            console.log('Closed all topic connections.')
        }

        ws.close(code, "Durable Object is closing WebSocket");
    }

    async fetch(request: Request): Promise<Response> {
        const url = new URL(request.url)

        // Establish a websocket connection between CLIENT <-> USER durable object (this).
        if (url.pathname === '/subscribe') {
            const webSocketPair = new WebSocketPair();
            const [client, server] = Object.values(webSocketPair);

            // Assign a random identifier to the socket and save the pair locally.
            const connectionId = crypto.randomUUID();
            this.connections.set(connectionId, server);
            this.ctx.acceptWebSocket(server);

            // Now that we have an active WS connection we want to subscribe and
            // listen to all of our topics we subscribe to.
            // Create an open websocket connection between this USER durable object
            // and each topic durable object we are subscribed to.
            this.changeSubscriptionStatus(true);

            return new Response(null, {
                status: 101,
                webSocket: client,
            });
        }

        return this.app.fetch(request);
    }

    async changeSubscriptionStatus(subscribe: boolean) {
        // If this function is called but we notice there are already subscribed
        // topics then we will not attempt to create new connections to topics
        // and instead eject early – only if the user is attempting to subscribe.
        if (this.topicConnections.size > 0 && subscribe) return;

        // If the user is subscribing to topics then we will pass the logic off
        // to our multiplex creation function to handle establishing that.
        // If the user is unsubscribing to topics then we should loop through
        // them ourselves and close each of them down between USER <-> topic.
        for (const topic of this.topics) {
            try {
                if (subscribe) {
                    this.createTopicSocketConnection(topic)
                } else {
                    const webSocket = this.topicConnections.get(topic);
                    if (webSocket) {
                        webSocket.close();
                        this.topicConnections.delete(topic);
                    }
                }
            } catch (error) {
                console.error(`Error with topic ${topic}:`, error);
            }
        }
    }

    async createTopicSocketConnection(topic: string, shardVersion: number = 0) {
        // Create a stub to another Durable Object based on the `topic` value
        // the user wants to subscribe to. In this example any number of users may
        // subscribe to any topic they want, think of it like a public chatroom.
        // Durable Objects can support over 32,000 web socket connections at a time
        // but the more limiting factor is usually the memory resources running the
        // DO's and not the web socket count.
        const stubId = this.env.TOPIC_DURABLE_OBJECT.idFromName(`${topic}_${shardVersion}`);
        const stub = this.env.TOPIC_DURABLE_OBJECT.get(stubId);

        // First do an RPC check to see if the shard is overloaded and if so we will
        // check the next shard continuously until one is available.
        const allowed: { success: boolean, limitReached?: boolean } = await stub.canSupportConnection();

        if (!allowed.success && !allowed.limitReached) {
            // Check to see if the next shard version can accept connections.
            await this.createTopicSocketConnection(topic, shardVersion + 1)
            return
        } else if (allowed.limitReached) {
            throw new Error("Maximum connections to this topic and all shards.");
        }

        // Now that we know we have an available shard to us, let's instantiate it in
        // case it hasn't been already so it knows what shard number it is as well as
        // any metadata it needs to know about (e.g. which topic it represents).
        await stub.init(shardVersion, { topic: topic })

        // To create a websocket connection between two Durable Objects we can
        // pass a request to a stubs `fetch` function which will interpret it
        // just as it would any other request. Here we are artificially creating
        // the request with the appropriate settings for establishing a new
        // web socket connection.
        const response = await stub.fetch('http://internal/subscribe', {
            headers: {
                'Upgrade': 'websocket',
                'Connection': 'Upgrade'
            }
        });
        
        // For socket connections, a 101 code typically means it was successful.
        if (response.status === 101) {
            const webSocket = response.webSocket;
            if (!webSocket) throw new Error('WebSocket connection failed');
            
            // This creates an outbound socket connection to another Durable Object
            // but it does NOT support hibernation. Outbound connections in DO's do
            // not currently support this feature.
            await webSocket.accept();
            
            // To consolidate our code efforts, forward non-hibernatable messages to
            // our hibernation supported webSocketMessage function.
            webSocket.addEventListener('message', (event: Record<string, any>) => {
                this.webSocketMessage(webSocket, event.data);
            });
            
            // And on error we'll just throw a console error for debugging purposes.
            webSocket.addEventListener('error', (error: unknown) => {
                console.error(`topic ${topic} WebSocket error:`, error);
            });
            
            // Add this topic/ws pair to our map for tracking and usage.
            this.topicConnections.set(topic, webSocket);
        }
    }
} 