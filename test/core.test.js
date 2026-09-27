import { test } from 'node:test';
import assert from 'node:assert/strict';

import { CsvParserStream, CsvStateMachine, parseCsvLine } from '../src/index.js';

/**
 * Collect all records emitted by a CsvParserStream. Resolves when 'end' fires.
 * @param {CsvParserStream} stream
 * @returns {Promise<string[][]>}
 */
function collect(stream) {
  return new Promise((resolve, reject) => {
    /** @type {string[][]} */
    const out = [];
    stream.on('data', (r) => out.push(r));
    stream.on('end', () => resolve(out));
    stream.on('error', reject);
  });
}

test('parseCsvLine: simple two-row input', () => {
  const rows = parseCsvLine('a,b\nc,d\n');
  assert.deepEqual(rows, [['a', 'b'], ['c', 'd']]);
});

test('parseCsvLine: no trailing newline still yields the last row', () => {
  const rows = parseCsvLine('a,b\nc,d');
  assert.deepEqual(rows, [['a', 'b'], ['c', 'd']]);
});

test('quoted field containing delimiter', () => {
  const rows = parseCsvLine('"a,b",c\n');
  assert.deepEqual(rows, [['a,b', 'c']]);
});

test('quoted field containing newline', () => {
  const rows = parseCsvLine('"line1\nline2",x\n');
  assert.deepEqual(rows, [['line1\nline2', 'x']]);
});

test('escaped quote inside quoted field (doubled quote)', () => {
  const rows = parseCsvLine('"she said ""hi""",x\n');
  assert.deepEqual(rows, [['she said "hi"', 'x']]);
});

test('CRLF line endings', () => {
  const rows = parseCsvLine('a,b\r\nc,d\r\n');
  assert.deepEqual(rows, [['a', 'b'], ['c', 'd']]);
});

test('lone CR as line ending', () => {
  const rows = parseCsvLine('a,b\rc,d\r');
  assert.deepEqual(rows, [['a', 'b'], ['c', 'd']]);
});

test('empty field and empty record', () => {
  const rows = parseCsvLine('a,,c\n,,\n');
  assert.deepEqual(rows, [['a', '', 'c'], ['', '', '']]);
});

test('custom delimiter', () => {
  const rows = parseCsvLine('a|b|c\n', { delimiter: '|' });
  assert.deepEqual(rows, [['a', 'b', 'c']]);
});

test('custom quote character', () => {
  const rows = parseCsvLine("'a''b',c\n", { quote: "'" });
  assert.deepEqual(rows, [["a'b", 'c']]);
});

test('stream: records emitted across chunk boundaries', async () => {
  const stream = new CsvParserStream();
  // Write in tiny pieces to exercise the state machine's resumability.
  stream.write('a,b');
  stream.write('\n"c');
  stream.write('d",e\n');
  stream.end();
  const rows = await collect(stream);
  assert.deepEqual(rows, [['a', 'b'], ['cd', 'e']]);
});

test('stream: quoted field split across chunks', async () => {
  const stream = new CsvParserStream();
  stream.write('"hel');
  stream.write('lo"');
  stream.write(',world\n');
  stream.end();
  const rows = await collect(stream);
  assert.deepEqual(rows, [['hello', 'world']]);
});

test('stream: doubled quote split across chunks', async () => {
  const stream = new CsvParserStream();
  stream.write('"ab"');
  stream.write('"cd",x\n');
  stream.end();
  const rows = await collect(stream);
  assert.deepEqual(rows, [['ab"cd', 'x']]);
});

test('stream: rejects non-string chunks', async () => {
  const stream = new CsvParserStream();
  await new Promise((resolve) => {
    stream.on('error', (err) => {
      assert.ok(err instanceof TypeError);
      resolve();
    });
    stream.write(Buffer.from('a,b\n'));
  });
});

test('constructor: rejects multi-char delimiter', () => {
  assert.throws(() => new CsvStateMachine({ delimiter: '||' }), TypeError);
});

test('constructor: rejects delimiter equal to quote', () => {
  assert.throws(() => new CsvStateMachine({ delimiter: '"', quote: '"' }), TypeError);
});

test('reset clears buffered state', () => {
  const m = new CsvStateMachine();
  m.push('a,b\n');
  m.reset();
  m.push('x,y\n');
  m.end();
  assert.deepEqual(m.takeRecords(), [['x', 'y']]);
});
