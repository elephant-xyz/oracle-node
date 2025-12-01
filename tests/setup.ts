import { expect } from "vitest";
import { allCustomMatcher } from "aws-sdk-client-mock-vitest";

// Extend Vitest's expect with AWS SDK client mock custom matchers
// These matchers provide better assertions for AWS SDK mock verification:
// - toHaveReceivedCommand(Command)
// - toHaveReceivedCommandWith(Command, input)
// - toHaveReceivedCommandTimes(Command, times)
// - toHaveReceivedCommandOnce(Command)
// - toHaveReceivedNthCommandWith(Command, n, input)
// - toHaveReceivedLastCommandWith(Command, input)
// - toHaveReceivedAnyCommand()
// - toHaveReceivedCommandExactlyOnceWith(Command, input)
expect.extend(allCustomMatcher);

