export type Env = {
    CLIENT_DURABLE_OBJECT: DurableObjectNamespace;
    TOPIC_DURABLE_OBJECT: DurableObjectNamespace<
        import('../durable-objects/topic').TopicDurableObject
    >
} 