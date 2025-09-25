# Shared Lambda Layer

This directory contains a shared AWS Lambda layer that provides common utilities across all Elephant workflow lambdas.

## Structure

```
shared/
├── nodejs/
│   └── node_modules/
│       └── @elephant/
│           └── shared/
│               ├── index.js      # Main ES module file with JSDoc
│               └── package.json  # Module metadata
├── package.json                  # Layer metadata
└── README.md                     # This file
```

## Usage

Import the shared module in your Lambda function:

```javascript
import { helloWorld, createLogEntry } from "@elephant/shared";

// Use the hello world function
const greeting = helloWorld();
console.log(greeting); // "Hello, World from shared module!"

// Use the logging utility
const logEntry = createLogEntry("component-name", "info", "operation_started", {
  details: "additional data",
});
```

## Available Functions

### `helloWorld()`

Returns a greeting string for testing purposes.

### `createLogEntry(component, level, msg, details)`

Creates a standardized log entry object with consistent structure.

**Parameters:**

- `component` (string): Component name (e.g., 'post', 'pre', 'starter')
- `level` (string): Log level ('info', 'error', 'debug')
- `msg` (string): Short message identifier
- `details` (object): Additional structured data

**Returns:** Structured log entry object

## Deployment

The layer is automatically built and deployed as part of the SAM template. All workflow lambdas have this layer attached.

## Development Setup

For local development, symlinks have been created in each lambda's `node_modules/@elephant/shared` pointing to this shared module. This allows:

1. **Type checking** to work correctly with JSDoc type annotations
2. **IntelliSense** and auto-completion in your IDE based on JSDoc
3. **Import resolution** during development

The module uses comprehensive **JSDoc type annotations** instead of separate TypeScript declaration files for type information.

## Adding New Functions

To add new functions to the shared module:

1. Add the function to `nodejs/node_modules/@elephant/shared/index.js`
2. Add comprehensive JSDoc documentation with type annotations
3. Export the function using ES module syntax (`export`)
4. Update this README with the new function documentation

## Module Type

This is an **ES module** (`"type": "module"` in package.json) to match the ES module usage in the workflow lambdas.

## Build Process

The layer uses SAM's build process with `BuildMethod: nodejs22.x` and `BuildArchitecture: arm64` to ensure compatibility with the workflow lambdas.
