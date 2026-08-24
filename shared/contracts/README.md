# Shared Contracts

This module contains versioned Pydantic models for all core event types.

## Current version
- Envelope: EventEnvelopeV1
- Lifecycle events:
  - OrderCreatedEventV1
  - InventoryReservedEventV1
  - InventoryFailedEventV1
  - PaymentAuthorizedEventV1
  - PaymentFailedEventV1
  - ShipmentCreatedEventV1
  - OrderCompletedEventV1
  - OrderFailedEventV1

## Rules
- All models use strict field validation with extra fields forbidden.
- All events must include the standard envelope fields.
- Breaking changes require V2 model names.
