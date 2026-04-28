# Inventory

## Potential Removals
- None at the moment.

## Desired Functionality
- Inspection utilities for listing entities, attributes, and relations, with optional filtering.
- Progress reporting for blob transfers and other long-running operations.
- Custom maximum pile size when creating piles.
- Consolidate shared blob-handling logic across `pile` and `store` commands.
- Centralize branch ID resolution helpers across CLI commands.
- CLI coverage for `pile merge` in the integration test suite.

## Discovered Issues
- Object store operations rely on an async runtime; consider synchronous alternatives.
- Preflight script and test suite take an unusually long time to run; investigate ways to reduce build and execution time.
