'use strict';

const express = require('express');
const { randomUUID } = require('crypto');

const host = process.env.SHIPPING_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.SHIPPING_PORT || '9000', 10);
const app = express();
app.use(express.json({ limit: '1mb' }));

const shipments = new Map();

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
    const shipment = {
      shipmentId,
      orderId,
      carrier: 'ACME_SHIP',
      trackingNumber: makeTrackingNumber(shipmentId),
      status: 'CREATED'
    };

    shipments.set(shipmentId, shipment);
    res.status(201).json(toShipmentView(shipment));
  } catch (error) {
    res.status(400).json({ error: error.message || 'Invalid request body' });
  }
});

app.get('/shipments/:shipmentId', (req, res) => {
  const shipmentId = decodeURIComponent(req.params.shipmentId);
  const shipment = shipments.get(shipmentId) || buildDefaultShipment(shipmentId);
  res.status(200).json(toShipmentView(shipment));
});

app.use((_req, res) => {
  res.status(404).json({ error: 'Not found' });
});

app.listen(port, host, () => {
  console.log(`shipping-service listening on http://${host}:${port}`);
});
