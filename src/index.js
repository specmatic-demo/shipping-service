'use strict';

const http = require('http');
const { randomUUID } = require('crypto');

const host = process.env.SHIPPING_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.SHIPPING_PORT || '9000', 10);

const shipments = new Map();

function sendJson(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(body)
  });
  res.end(body);
}

function parseBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';

    req.on('data', (chunk) => {
      body += chunk;
      if (body.length > 1024 * 1024) {
        reject(new Error('Payload too large'));
      }
    });

    req.on('end', () => {
      if (!body) {
        resolve({});
        return;
      }

      try {
        resolve(JSON.parse(body));
      } catch (_error) {
        reject(new Error('Invalid JSON body'));
      }
    });

    req.on('error', reject);
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

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);

  if (req.method === 'POST' && url.pathname === '/shipments') {
    try {
      const payload = await parseBody(req);
      const orderId = typeof payload.orderId === 'string' ? payload.orderId.trim() : '';
      const destinationPostalCode =
        typeof payload.destinationPostalCode === 'string' ? payload.destinationPostalCode.trim() : '';

      if (!orderId) {
        sendJson(res, 400, { error: 'orderId must be a non-empty string' });
        return;
      }

      if (!destinationPostalCode) {
        sendJson(res, 400, { error: 'destinationPostalCode must be a non-empty string' });
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
      sendJson(res, 201, toShipmentView(shipment));
    } catch (error) {
      sendJson(res, 400, { error: error.message || 'Invalid request body' });
    }

    return;
  }

  const shipmentIdMatch = url.pathname.match(/^\/shipments\/([^/]+)$/);
  if (req.method === 'GET' && shipmentIdMatch) {
    const shipmentId = decodeURIComponent(shipmentIdMatch[1]);
    const shipment = shipments.get(shipmentId) || buildDefaultShipment(shipmentId);
    sendJson(res, 200, toShipmentView(shipment));
    return;
  }

  sendJson(res, 404, { error: 'Not found' });
});

server.listen(port, host, () => {
  console.log(`shipping-service listening on http://${host}:${port}`);
});
