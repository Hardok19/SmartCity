# SmartCity — Project TODO

## Overview
Concise, actionable subtasks for the SmartCity project. Break work into small, verifiable steps prioritized for an initial MVP.

---

## High Priority (MVP)
- [x] Create initial `TODO.md` (this file)
- [ ] Define project scope and success criteria
- [ ] Add short project description and goals

### Development environment
- [ ] Initialize repository basics (`.gitignore`, license)
- [ ] Create Python virtual environment (`venv`) or Node env
- [ ] Add dependency manifest (`requirements.txt` / `pyproject.toml` / `package.json`)

### Ingestion (data pipeline)
- [ ] Define data sources and formats (sensors, feeds)
- [ ] Implement a sensor simulator for dev/testing
- [ ] Implement ingestion service (collector endpoint or consumer)
- [ ] Persist raw data (filesystem folder or lightweight DB)

### Backend API
- [ ] Design OpenAPI / API spec (health, ingest, devices, metrics)
- [ ] Implement core endpoints (`/health`, `/ingest`, `/devices`, `/metrics`)
- [ ] Add authentication (JWT) and basic role checks

## Medium Priority

### Frontend
- [ ] Scaffold frontend app (React or Vue)
- [ ] Create dashboard pages: overview, device list, map view
- [ ] Add realtime or poll updates for key metrics

### Testing & CI
- [ ] Add unit tests for backend services
- [ ] Add integration tests: ingestion → storage → API
- [ ] Setup CI pipeline (GitHub Actions) to run tests and lint

## Low Priority / Backlog
- [ ] Add monitoring, logging, and alerting
- [ ] Advanced analytics / ML model experiments
- [ ] Integrate external city services (transit, utilities)
- [ ] Mobile app or PWA
- [ ] Performance and scaling improvements (k8s, queues)

## Documentation & Contribution
- [ ] Write `README.md` with quickstart and example commands
- [ ] Add `CONTRIBUTING.md` and `CODE_OF_CONDUCT.md`
- [ ] Add deployment docs (Dockerfile, docker-compose, cloud notes)

## Notes
- Break work into small PRs and add issue links when possible.
- Start with ingestion → API → UI to get a visible, testable MVP quickly.

