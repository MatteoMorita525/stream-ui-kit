# CSV Stream Parser

A Node.js transform stream that parses CSV text into record arrays, handling quoted fields, escaped quotes (doubled quote characters), and configurable single-character delimiters.

## Usage

```js
import { CsvParserStream, parseCsvLine } from 'csv-stream-parser';

// Streaming usage — input chunks must be strings.
const stream = new CsvParserStream({ delimiter: ',', quote: '"' });
stream.on('data', (record) => {
  // record is a string[], e.g. ['a', 'b']
});
stream.write('a,b\n');
stream.end();

// Synchronous convenience for small inputs.
const rows = parseCsvLine('"hello, world",1\n"she said ""hi""",2\n');
// => [['hello, world', '1'], ['she said "hi"', '2']]
```

## Why

The built-in CSV handling in most CSV libraries either buffers the entire file or papers over quoting rules. This library exists for the case where you are piping a large CSV through a stream pipeline and need each row as soon as it is complete, without holding the whole file in memory. The trade-off is that the stream operates in object mode (one array per record) and expects string input — it does not decode Buffers itself, so pair it with `setEncoding` or a decode step.

## Edge cases

- **Line endings:** `\r\n`, `\n`, and a lone `\r` are all treated as record terminators. A `\n` immediately following a `\r` is consumed as part of the same terminator and does not produce an empty record.
- **Quoted newlines:** inside a quoted field, `\n` and `\r` are literal field content, not record boundaries.
- **Doubled quotes:** a `""` inside a quoted field is a single literal `"`. This follows RFC 4180.
- **Unterminated quotes:** if input ends while inside a quoted field, the accumulated content is returned as-is rather than throwing. This is a recovery choice for stream resilience, not strict validation.
- **Mid-field quotes:** a quote character that appears after non-quote content in an unquoted field is treated as a literal character, not as the start of a quoted section.
- **No trailing newline:** the final record is still emitted.

## Exports

- `CsvParserStream` — a `Transform` stream (object mode). Constructor accepts `{ delimiter?: string, quote?: string }`.
- `parseCsvLine(input: string, opts?)` — synchronous helper returning `string[][]`.
- `CsvStateMachine` — the underlying state machine, exposed for direct driving.

## Tests

```
node --test
```
