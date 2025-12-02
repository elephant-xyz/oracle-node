# Dev environment tips

- When working with JavaScript always create JSDoc with very detailed type definitions for functions input/output and
  variable types.
- NEVER use `any` type as an option no matter the case
- Run `npm run typecheck` after making any changes to the code
- Make sure to cover code changes with tests
- Run `npm run test` to run tests
- NEVER use `FilterExpression` when working with DynamoDB.
