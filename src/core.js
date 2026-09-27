/**
 * CSV stream parser.
 *
 * Design notes:
 *
 * The parser is a single-character state machine. It does not use regex for
 * field splitting because quoted fields may contain newlines, delimiters, and
 * the quote character itself (doubled). A regex approach forces you to peek
 * across chunk boundaries, which is fiddly; a character loop keeps the state
 * explicit and makes backpressure-free streaming trivial.
 *
 * RFC 4180 is the reference for field quoting: a field is wrapped in double
 * quotes, and a literal double quote inside a quoted field is represented as
 * two consecutive double quotes. We support an arbitrary single-character
 * delimiter (comma by default) and an arbitrary single-character quote char
 * (double quote by default). Multi-character delimiters are out of scope —
 * supporting them well requires a different buffering strategy and the common
 * case is a single byte.
 *
 * Line endings: we treat \r\n, \n, and a lone \r as record terminators. Inside
 * a quoted field, newlines are literal data, not record boundaries.
 */

import { Transform } from 'node:stream';

/**
 * Stateful CSV parser. Feed it characters via push() and pull complete records
 * via takeRecords(). This separation lets the transform stream drive the loop
 * without exposing internal state.
 */
export class CsvStateMachine {
  /**
   * @param {{delimiter?: string, quote?: string}} [opts]
   */
  constructor(opts = {}) {
    const delimiter = opts.delimiter ?? ',';
    const quote = opts.quote ?? '"';

    if (delimiter.length !== 1) {
      throw new TypeError('delimiter must be a single character');
    }
    if (quote.length !== 1) {
      throw new TypeError('quote must be a single character');
    }
    if (delimiter === quote) {
      throw new TypeError('delimiter and quote must differ');
    }

 this._delimiter = delimiter;
    this._quote = quote;

    this._field = '';
    this._record = [];
    this._records = [];
    this._inQuotes = false;
    this._fieldStarted = false; // have we seen any char for the current field?
    this._quoteSeen = false;    // did we just close a quoted section?
    this._lastWasCr = false;    // did we just see a \r (for \r\n handling)?
  }

  /**
   * Feed a chunk of text. Characters are processed one at a time so that
   * record boundaries are emitted as soon as they are complete.
   *
   * @param {string} text
   */
  push(text) {
    for (let i = 0; i < text.length; i++) {
      this._consume(text[i]);
    }
  }

  /**
   * Signal that the input has ended. If there is a partial record in flight,
   * it is flushed as a final record. This matches the behaviour of most CSV
   * readers: a file with no trailing newline still yields its last row.
   */
  end() {
    // If we were inside quotes when input ends, treat the accumulated field
    // content as literal data. This is a recovery choice — strictly, an
    // unterminated quote is malformed. We prefer to return the bytes rather
    // than throw, because stream consumers often cannot easily surface a
    // parse error mid-pipe.
    //
    // Only flush a final record if there is a partial record in flight. After
    // a record terminator, _fieldStarted is false and _record is empty, so
    // there is nothing to flush — emitting an extra empty record would be
    // wrong.
    if (this._fieldStarted || this._record.length > 0 || this._inQuotes) {
      this._record.push(this._field);
      this._records.push(this._record);
    }
    this._field = '';
    this._record = [];
    this._inQuotes = false;
    this._fieldStarted = false;
    this._quoteSeen = false;
    this._lastWasCr = false;
  }

  /** @returns {string[][]} Completed records. The internal buffer is cleared. */
  takeRecords() {
    const out = this._records;
    this._records = [];
    return out;
  }

  /**
   * Reset to the initial state. Useful for reusing the object across streams.
   */
  reset() {
    this._field = '';
    this._record = [];
    this._records = [];
    this._inQuotes = false;
    this._fieldStarted = false;
    this._quoteSeen = false;
    this._lastWasCr = false;
  }

  /** @param {string} ch */
  _consume(ch) {
    if (this._inQuotes) {
      this._consumeInQuotes(ch);
      return;
    }
    this._consumeOutsideQuotes(ch);
  }

  /** @param {string} ch */
  _consumeInQuotes(ch) {
    if (ch === this._quote) {
      // Could be the closing quote, or the first of a doubled quote. We
      // can't tell yet — defer the decision by flipping state and setting
      // a flag. The next character resolves it.
      this._inQuotes = false;
      this._quoteSeen = true;
      return;
    }
    this._field += ch;
  }

  /** @param {string} ch */
  _consumeOutsideQuotes(ch) {
    if (ch === this._quote && !this._quoteSeen) {
      // Opening quote of a quoted field. We only honour it at the start of
      // a field; a quote mid-field (after non-quote content) is treated as
      // a literal character. This is the common CSV convention and avoids
      // ambiguity in inputs like a,b"c,d.
      if (!this._fieldStarted) {
        this._inQuotes = true;
        this._fieldStarted = true;
        return;
      }
      // Fall through: literal quote.
    }

    if (this._quoteSeen && ch === this._quote) {
      // Doubled quote inside a quoted field -> literal quote.
      this._field += this._quote;
      this._quoteSeen = false;
      this._inQuotes = true;
      return;
    }

    if (this._quoteSeen) {
      // The previous quote really was the closing quote. Any non-quote
      // character now means we are past the field content; fall through to
      // delimiter/newline handling.
      this._quoteSeen = false;
    }

    if (ch === this._delimiter) {
      this._record.push(this._field);
      this._field = '';
      this._fieldStarted = false;
      this._lastWasCr = false;
      return;
    }

    if (ch === '\n' || ch === '\r') {
      this._finishRecord(ch);
      return;
    }

    this._field += ch;
    this._fieldStarted = true;
    this._lastWasCr = false;
  }

  /** @param {string} ch */
  _finishRecord(ch) {
    // \r\n is two characters; the \n after a \r would produce an empty record.
    // Guard against that by checking whether we just saw a \r and this is the
    // following \n — in that case, consume it silently.
    if (ch === '\n' && this._lastWasCr) {
      this._lastWasCr = false;
      return;
    }
    this._record.push(this._field);
    this._records.push(this._record);
    this._record = [];
    this._field = '';
    this._fieldStarted = false;
    this._lastWasCr = ch === '\r';
  }
}

/**
 * Convenience: parse a complete CSV string into an array of records.
 * Intended for small inputs that already fit in memory.
 *
 * @param {string} input
 * @param {{delimiter?: string, quote?: string}} [opts]
 * @returns {string[][]}
 */
export function parseCsvLine(input, opts) {
  const m = new CsvStateMachine(opts);
  m.push(input);
  m.end();
  return m.takeRecords();
}

/**
 * Transform stream that parses CSV text into record arrays.
 *
 * Input chunks should be strings (string mode). The stream does not decode
 * Buffers itself; pair it with `setEncoding` or a preceding decode stream.
 * Output chunks are arrays of strings (one record per chunk).
 */
export class CsvParserStream extends Transform {
  /**
   * @param {{delimiter?: string, quote?: string}} [opts]
   */
  constructor(opts = {}) {
    super({
      objectMode: true,
      // We emit one record per push, so highWaterMark in object mode is fine
      // at its default. No need to override.
    });
    this._machine = new CsvStateMachine(opts);
  }

  _transform(chunk, _encoding, callback) {
    if (typeof chunk !== 'string') {
      callback(new TypeError('CsvParserStream expects string chunks; got ' + typeof chunk));
      return;
    }
    this._machine.push(chunk);
    for (const record of this._machine.takeRecords()) {
      this.push(record);
    }
    callback();
  }

  _flush(callback) {
    this._machine.end();
    for (const record of this._machine.takeRecords()) {
      this.push(record);
    }
    callback();
  }
}
