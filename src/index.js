/**
 * CSV Stream Parser — public entry point.
 *
 * Re-exports the transform stream and the synchronous core parser so consumers can
 * either pipe data through the stream or drive the state machine directly.
 */
export { CsvParserStream } from './core.js';
export { CsvStateMachine, parseCsvLine } from './core.js';
