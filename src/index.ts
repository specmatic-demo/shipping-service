import { randomUUID } from 'node:crypto';
import express, { type Request, type Response } from 'express';
import mqtt from 'mqtt';

const host = process.env.SHIPPING_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.SHIPPING_PORT || '9000', 10);
const analyticsMqttUrl = process.env.ANALYTICS_MQTT_URL || 'mqtt://localhost:1883';
const analyticsNotificationTopic = process.env.ANALYTICS_NOTIFICATION_TOPIC || 'notification/user';
const app = express();
app.use(express.json({ limit: '1mb' }));

type Shipment = {
  shipmentId: string;
  orderId: string;
  carrier: string;
  trackingNumber: string;
  status: 'CREATED' | 'IN_TRANSIT' | 'DELIVERED' | 'DELAYED' | 'CANCELLED';
  cancelledAt?: string;
};

type AnalyticsNotificationEvent = {
  notificationId: string;
  requestId: string;
  title: string;
  body: string;
  priority: 'LOW' | 'NORMAL' | 'HIGH';
};

const shipments = new Map<string, Shipment>();

function publishAnalyticsNotification(event: AnalyticsNotificationEvent): void {
  const client = mqtt.connect(analyticsMqttUrl, { reconnectPeriod: 0, connectTimeout: 1000 });
  const payload = JSON.stringify(event);
  let completed = false;

  const done = (): void => {
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
    client.publish(analyticsNotificationTopic, payload, { qos: 1 }, (error?: Error | null) => {
      if (error) {
        console.error(`Failed to publish analytics notification on ${analyticsNotificationTopic}: ${error.message}`);
      }

      clearTimeout(timeout);
      done();
    });
  });

  client.once('error', (error: Error) => {
    console.error(`Failed to connect to analytics MQTT broker (${analyticsMqttUrl}): ${error.message}`);
    clearTimeout(timeout);
    done();
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
});
