export type Shipment = {
  shipmentId: string;
  orderId: string;
  carrier: string;
  trackingNumber: string;
  status: 'CREATED' | 'IN_TRANSIT' | 'DELIVERED' | 'DELAYED' | 'CANCELLED';
  cancelledAt?: string;
};

export type AnalyticsNotificationEvent = {
  notificationId: string;
  requestId: string;
  title: string;
  body: string;
  priority: 'LOW' | 'NORMAL' | 'HIGH';
};

export type DispatchCommandEvent = {
  messageId: string;
  requestId: string;
  orderId: string;
  carrier: string;
  requestedAt: string;
};

export type FulfillmentReplyEvent = {
  messageId: string;
  requestId: string;
  orderId: string;
  status: 'ACCEPTED' | 'REJECTED' | 'PARTIAL';
  repliedAt: string;
  trackingId?: string;
  rejectionReason?: string;
};

export type ShippingShippedEvent = {
  eventId: string;
  orderId: string;
  shipmentId: string;
  status: 'CREATED' | 'PICKED_UP' | 'IN_TRANSIT' | 'OUT_FOR_DELIVERY' | 'DELIVERED' | 'FAILED';
  title: string;
  body: string;
  priority?: 'LOW' | 'NORMAL' | 'HIGH';
  occurredAt: string;
};
