/**
 * @typedef {Pick<Console, "log">} ConsoleLike
 * Logger shape that exposes the `log` function matching the global Console API.
 */

/**
 * Emits a greeting message using the provided logger instance.
 *
 * @param {ConsoleLike} logger Console-compatible logging target.
 * @param {string} message Greeting message to emit verbatim.
 * @returns {void}
 */
export function emitGreeting(logger, message) {
  logger.log(message);
}

/** @type {string} */
export const greetingMessage = "heello world";

emitGreeting(console, greetingMessage);
