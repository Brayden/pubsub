export enum QoS {
    qos_0 = 0,  // Best effort, "at most once" | no ack required | message duplication possible on network retries
    qos_1 = 1,  // Guaranteed delivery, "at least once" | ack required (PUBACK) | message duplication possible
    qos_2 = 2   // Guaranteed once only, "exactly once" | ack required (PUBREC, PUBREL, PUBCOMP) | no duplication
}

export enum Step {
    none = 0,
    PUBACK = 'PUBACK',
    PUBREC = 'PUBREC',
    PUBREL = 'PUBREL',
    PUBCOMP = 'PUBCOMP'
}

export type MessagePayload = {
    // Idempotent message ID that uniquely identifies this message
    messageId: string;
    // Which topic this message should be directed to
    topic: string;
    // Customizable payload provided by the user
    payload: Record<string, any>;
    // Quality level this should adhere to
    qos: QoS;
    // What status step the message is in, if any
    step?: undefined | Step
}

export enum SubscriptionSource {
    client,
    topic
}
export type SubscribePayload = {
    source: SubscriptionSource;
    topics?: string[];
    // clientId?: string;
    // topic?: string;
}