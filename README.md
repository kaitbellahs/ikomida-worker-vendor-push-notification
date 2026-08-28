# ikomida-worker-vendor-push-notification

Push delivery to vendors.

> Part of the **iKomida** platform. See **[ikomida-k8s-config](https://github.com/kaitbellahs/ikomida-k8s-config)** for the architecture overview of all 31 repositories.

---

## Role

Separate from the client push worker on purpose: a vendor tablet waiting on an incoming order has different urgency and different failure consequences than a customer being told their food is on the way. Splitting the queues means one cannot starve the other.

## Queue

Consumes `VENDOR_PUSH_NOTIFICATION_QUEUE` from RabbitMQ. Messages are processed with bounded retries and acknowledged only on success; failures are negatively acknowledged so nothing is lost silently.

## Stack

TypeScript (ESM) · amqplib · Sequelize · rollup · Docker · Kubernetes

Depends on [`@ikomida/shared-types`](https://github.com/kaitbellahs/ikomida-shared-types), [`@ikomida/shared-backend`](https://github.com/kaitbellahs/ikomida-shared-backend) and [`@ikomida/shared-logics`](https://github.com/kaitbellahs/ikomida-shared-logics).

## Build

```bash
yarn install
yarn build
yarn worker     # start consuming
```

## Status

Built in 2022. The platform is no longer deployed; this repository is published as a record of the work. **The commit history predates generative AI coding assistants.**

## License

Licensed under the [Apache License 2.0](LICENSE) — free for commercial use, provided the copyright notice and [NOTICE](NOTICE) are retained.

Copyright 2022 Khalid Ait Bellahs.
