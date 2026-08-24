# Kafka Distributed Order Processing

Learning-first backend project focused on Kafka and Kubernetes using FastAPI-based mock services.

## Repository layout
- `services/order-service` - Order intake API and event producer.
- `services/inventory-service` - Mock inventory reservation consumer/producer.
- `services/payment-service` - Mock payment decision consumer/producer.
- `services/shipping-service` - Mock shipping creation consumer/producer.
- `shared/contracts` - Shared event contracts and validation models.
- `shared/common` - Shared utility code (config/logging helpers).
- `deployments/k8s/base` - Base Kubernetes manifests.
- `deployments/k8s/overlays/local` - Local Kubernetes overlay customizations.
- `infra/docker` - Local infrastructure assets.
- `sample-data` - Generated dummy data payloads and event streams.
- `scripts` - Utility scripts (for example, data generation).
- `docs` - Architecture, conventions, and execution docs.

## Current phase
Phase 0 (foundation) started.

See:
- `DEVELOPMENT_PLAN.md`
- `docs/PHASE0_FOUNDATION.md`
- `docs/CONVENTIONS.md`
