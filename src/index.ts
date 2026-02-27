import { randomUUID } from 'node:crypto';
import express, { type Request, type Response } from 'express';
import { Kafka, type Consumer, type Producer } from 'kafkajs';
import type {
  AnalyticsNotificationEvent,
  DispatchCommandEvent,
  FulfillmentReplyEvent,
  Shipment
} from './types';

const host = process.env.SHIPPING_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.SHIPPING_PORT || '9000', 10);
const analyticsNotificationTopic = process.env.ANALYTICS_NOTIFICATION_TOPIC || 'notification.user';
const shippingKafkaBrokers = (process.env.SHIPPING_KAFKA_BROKERS || 'localhost:9092')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const dispatchCommandTopic = process.env.SHIPPING_DISPATCH_COMMAND_TOPIC || 'queue.shipping.dispatch.command';
const fulfillmentReplyTopic = process.env.SHIPPING_FULFILLMENT_REPLY_TOPIC || 'queue.order.fulfillment.reply';
const app = express();
app.use(express.json({ limit: '1mb' }));

const shipments = new Map<string, Shipment>();
const kafka = new Kafka({
  clientId: 'shipping-service',
  brokers: shippingKafkaBrokers
});
const dispatchConsumer: Consumer = kafka.consumer({ groupId: 'shipping-service-dispatch' });
const fulfillmentProducer: Producer = kafka.producer();
let kafkaConnected = false;

function publishAnalyticsNotification(event: AnalyticsNotificationEvent): void {
  void fulfillmentProducer.send({
    topic: analyticsNotificationTopic,
    messages: [{ key: event.requestId, value: JSON.stringify(event) }]
  }).catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Failed to publish analytics notification on ${analyticsNotificationTopic}: ${message}`);
  });
}

function isDispatchCommandEvent(value: unknown): value is DispatchCommandEvent {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return false;
  }

  const payload = value as Record<string, unknown>;
  return (
    typeof payload.messageId === 'string' &&
    typeof payload.requestId === 'string' &&
    typeof payload.orderId === 'string' &&
    typeof payload.carrier === 'string' &&
    typeof payload.requestedAt === 'string'
  );
}

function buildFulfillmentReply(command: DispatchCommandEvent): FulfillmentReplyEvent {
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

async function startDispatchCommandConsumer(): Promise<void> {
  await dispatchConsumer.connect();
  await fulfillmentProducer.connect();
  kafkaConnected = true;

  await dispatchConsumer.subscribe({ topic: dispatchCommandTopic, fromBeginning: false });
  await dispatchConsumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) {
        return;
      }

      let payload: unknown;
      try {
        payload = JSON.parse(message.value.toString('utf8')) as unknown;
      } catch {
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

function makeTrackingNumber(shipmentId: string): string {
  return `TRK-${shipmentId.replace(/-/g, '').slice(0, 12).toUpperCase()}`;
}

function toShipmentView(shipment: Shipment): Shipment {
  return {
    shipmentId: shipment.shipmentId,
    orderId: shipment.orderId,
    carrier: shipment.carrier,
    trackingNumber: shipment.trackingNumber,
    status: shipment.status
  };
}

function buildDefaultShipment(shipmentId: string): Shipment {
  return {
    shipmentId,
    orderId: `order-${shipmentId}`,
    carrier: 'ACME_SHIP',
    trackingNumber: makeTrackingNumber(shipmentId),
    status: 'IN_TRANSIT'
  };
}

app.post('/shipments', (req: Request, res: Response) => {
  try {
    const payload = req.body || {};
    const orderId = typeof payload.orderId === 'string' ? payload.orderId.trim() : '';
    const destinationPostalCode =
      typeof payload.destinationPostalCode === 'string' ? payload.destinationPostalCode.trim() : '';

    if (!orderId) {
      res.status(400).json({ error: 'orderId must be a non-empty string' });
      return;
    }

    if (!destinationPostalCode) {
      res.status(400).json({ error: 'destinationPostalCode must be a non-empty string' });
      return;
    }

    const shipmentId = randomUUID();
    const shipment: Shipment = {
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
  } catch (error: unknown) {
    res.status(400).json({ error: error instanceof Error ? error.message : 'Invalid request body' });
  }
});

app.get('/shipments/:shipmentId', (req: Request, res: Response) => {
  const shipmentId = decodeURIComponent(req.params.shipmentId);
  const shipment = shipments.get(shipmentId) || buildDefaultShipment(shipmentId);
  res.status(200).json(toShipmentView(shipment));
});

app.get('/shipments', (req: Request, res: Response) => {
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

app.post('/shipments/:shipmentId/cancel', (req: Request, res: Response) => {
  const shipmentId = decodeURIComponent(req.params.shipmentId);
  const existing = shipments.get(shipmentId) || buildDefaultShipment(shipmentId);
  const cancelled: Shipment = {
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

app.use((_req: Request, res: Response) => {
  res.status(404).json({ error: 'Not found' });
});

app.listen(port, host, () => {
  console.log(`shipping-service listening on http://${host}:${port}`);
  void startDispatchCommandConsumer()
    .then(() => {
      console.log(
        `shipping-service kafka listener started on brokers=${shippingKafkaBrokers.join(',')} topic=${dispatchCommandTopic}`
      );
    })
    .catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`shipping-service kafka listener failed to start: ${message}`);
    });
});

async function shutdown(): Promise<void> {
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
