import { randomUUID } from 'node:crypto';
import express from 'express';
import { Kafka } from 'kafkajs';
import mqtt from 'mqtt';
const host = process.env.SHIPPING_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.SHIPPING_PORT || '9000', 10);
const analyticsMqttUrl = process.env.ANALYTICS_MQTT_URL || 'mqtt://localhost:1883';
const analyticsNotificationTopic = process.env.ANALYTICS_NOTIFICATION_TOPIC || 'notification/user';
const shippingKafkaBrokers = (process.env.SHIPPING_KAFKA_BROKERS || 'localhost:9092')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
const dispatchCommandTopic = process.env.SHIPPING_DISPATCH_COMMAND_TOPIC || 'queue.shipping.dispatch.command';
const fulfillmentReplyTopic = process.env.SHIPPING_FULFILLMENT_REPLY_TOPIC || 'queue.order.fulfillment.reply';
const app = express();
app.use(express.json({ limit: '1mb' }));
const shipments = new Map();
const kafka = new Kafka({
    clientId: 'shipping-service',
    brokers: shippingKafkaBrokers
});
const dispatchConsumer = kafka.consumer({ groupId: 'shipping-service-dispatch' });
const fulfillmentProducer = kafka.producer();
let kafkaConnected = false;
function publishAnalyticsNotification(event) {
    const client = mqtt.connect(analyticsMqttUrl, { reconnectPeriod: 0, connectTimeout: 1000 });
    const payload = JSON.stringify(event);
    let completed = false;
    const done = () => {
        if (completed) {
            return;
        }
        completed = true;
        client.end(true);
    };
    const timeout = setTimeout(() => {
        done();
    }, 1500);
    client.once('connect', () => {
        client.publish(analyticsNotificationTopic, payload, { qos: 1 }, (error) => {
            if (error) {
                console.error(`Failed to publish analytics notification on ${analyticsNotificationTopic}: ${error.message}`);
            }
            clearTimeout(timeout);
            done();
        });
    });
    client.once('error', (error) => {
        console.error(`Failed to connect to analytics MQTT broker (${analyticsMqttUrl}): ${error.message}`);
        clearTimeout(timeout);
        done();
    });
}
function isDispatchCommandEvent(value) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        return false;
    }
    const payload = value;
    return (typeof payload.messageId === 'string' &&
        typeof payload.requestId === 'string' &&
        typeof payload.orderId === 'string' &&
        typeof payload.carrier === 'string' &&
        typeof payload.requestedAt === 'string');
}
function buildFulfillmentReply(command) {
    if (Number.isNaN(Date.parse(command.requestedAt))) {
        return {
            messageId: randomUUID(),
            requestId: command.requestId,
            orderId: command.orderId,
            status: 'REJECTED',
            repliedAt: new Date().toISOString(),
            rejectionReason: 'Invalid requestedAt'
        };
    }
    return {
        messageId: randomUUID(),
        requestId: command.requestId,
        orderId: command.orderId,
        status: 'ACCEPTED',
        repliedAt: new Date().toISOString(),
        trackingId: makeTrackingNumber(randomUUID())
    };
}
async function startDispatchCommandConsumer() {
    await dispatchConsumer.connect();
    await fulfillmentProducer.connect();
    kafkaConnected = true;
    await dispatchConsumer.subscribe({ topic: dispatchCommandTopic, fromBeginning: false });
    await dispatchConsumer.run({
        eachMessage: async ({ message }) => {
            if (!message.value) {
                return;
            }
            let payload;
            try {
                payload = JSON.parse(message.value.toString('utf8'));
            }
            catch {
                return;
            }
            if (!isDispatchCommandEvent(payload)) {
                return;
            }
            const command = payload;
            const reply = buildFulfillmentReply(command);
            if (reply.status === 'ACCEPTED') {
                const shipmentId = randomUUID();
                shipments.set(shipmentId, {
                    shipmentId,
                    orderId: command.orderId,
                    carrier: command.carrier || 'ACME_SHIP',
                    trackingNumber: reply.trackingId || makeTrackingNumber(shipmentId),
                    status: 'CREATED'
                });
            }
            await fulfillmentProducer.send({
                topic: fulfillmentReplyTopic,
                messages: [{ key: command.orderId, value: JSON.stringify(reply) }]
            });
        }
    });
}
function makeTrackingNumber(shipmentId) {
    return `TRK-${shipmentId.replace(/-/g, '').slice(0, 12).toUpperCase()}`;
}
function toShipmentView(shipment) {
    return {
        shipmentId: shipment.shipmentId,
        orderId: shipment.orderId,
        carrier: shipment.carrier,
        trackingNumber: shipment.trackingNumber,
        status: shipment.status
    };
}
function buildDefaultShipment(shipmentId) {
    return {
        shipmentId,
        orderId: `order-${shipmentId}`,
        carrier: 'ACME_SHIP',
        trackingNumber: makeTrackingNumber(shipmentId),
        status: 'IN_TRANSIT'
    };
}
app.post('/shipments', (req, res) => {
    try {
        const payload = req.body || {};
        const orderId = typeof payload.orderId === 'string' ? payload.orderId.trim() : '';
        const destinationPostalCode = typeof payload.destinationPostalCode === 'string' ? payload.destinationPostalCode.trim() : '';
        if (!orderId) {
            res.status(400).json({ error: 'orderId must be a non-empty string' });
            return;
        }
        if (!destinationPostalCode) {
            res.status(400).json({ error: 'destinationPostalCode must be a non-empty string' });
            return;
        }
        const shipmentId = randomUUID();
        const shipment = {
            shipmentId,
            orderId,
            carrier: 'ACME_SHIP',
            trackingNumber: makeTrackingNumber(shipmentId),
            status: 'CREATED'
        };
        shipments.set(shipmentId, shipment);
        publishAnalyticsNotification({
            notificationId: randomUUID(),
            requestId: shipmentId,
            title: 'ShipmentCreated',
            body: `Shipment ${shipmentId} created for order ${orderId}`,
            priority: 'NORMAL'
        });
        res.status(201).json(toShipmentView(shipment));
    }
    catch (error) {
        res.status(400).json({ error: error instanceof Error ? error.message : 'Invalid request body' });
    }
});
app.get('/shipments/:shipmentId', (req, res) => {
    const shipmentId = decodeURIComponent(req.params.shipmentId);
    const shipment = shipments.get(shipmentId) || buildDefaultShipment(shipmentId);
    res.status(200).json(toShipmentView(shipment));
});
app.get('/shipments', (req, res) => {
    const orderId = typeof req.query.orderId === 'string' ? req.query.orderId : undefined;
    let list = Array.from(shipments.values());
    if (list.length === 0) {
        list = [{
                shipmentId: randomUUID(),
                orderId: orderId || 'order-default',
                carrier: 'ACME_SHIP',
                trackingNumber: makeTrackingNumber(randomUUID()),
                status: 'IN_TRANSIT'
            }];
    }
    const filtered = orderId ? list.filter((shipment) => shipment.orderId === orderId) : list;
    res.status(200).json(filtered.map((shipment) => toShipmentView(shipment)));
});
app.post('/shipments/:shipmentId/cancel', (req, res) => {
    const shipmentId = decodeURIComponent(req.params.shipmentId);
    const existing = shipments.get(shipmentId) || buildDefaultShipment(shipmentId);
    const cancelled = {
        ...existing,
        status: 'CANCELLED',
        cancelledAt: new Date().toISOString()
    };
    shipments.set(shipmentId, cancelled);
    publishAnalyticsNotification({
        notificationId: randomUUID(),
        requestId: shipmentId,
        title: 'ShipmentCancelled',
        body: `Shipment ${shipmentId} cancelled`,
        priority: 'NORMAL'
    });
    res.status(200).json(toShipmentView(cancelled));
});
app.use((_req, res) => {
    res.status(404).json({ error: 'Not found' });
});
app.listen(port, host, () => {
    console.log(`shipping-service listening on http://${host}:${port}`);
    void startDispatchCommandConsumer()
        .then(() => {
        console.log(`shipping-service kafka listener started on brokers=${shippingKafkaBrokers.join(',')} topic=${dispatchCommandTopic}`);
    })
        .catch((error) => {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`shipping-service kafka listener failed to start: ${message}`);
    });
});
async function shutdown() {
    if (kafkaConnected) {
        await Promise.allSettled([dispatchConsumer.disconnect(), fulfillmentProducer.disconnect()]);
    }
    process.exit(0);
}
process.once('SIGINT', () => {
    void shutdown();
});
process.once('SIGTERM', () => {
    void shutdown();
});
