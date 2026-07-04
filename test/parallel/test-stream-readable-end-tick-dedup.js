'use strict';
const common = require('../common');
const assert = require('assert');
const { PassThrough, Transform } = require('stream');

// Verify that ending a readable never schedules a second endReadableNT()
// nextTick while one is already pending. On the short-lived flowing path
// endReadable() can be invoked several times after end (from flow() and
// resume_()); the kEndScheduled guard must dedupe the scheduling while
// still emitting 'end' exactly once per stream.

const originalNextTick = process.nextTick;
const pendingStates = new Set();
let endReadableNTScheduled = 0;

process.nextTick = function nextTick(callback, ...args) {
  if (typeof callback === 'function' && callback.name === 'endReadableNT') {
    const state = args[0];
    assert.ok(
      !pendingStates.has(state),
      'endReadableNT scheduled while a previous tick for the same ' +
        'stream is still pending'
    );
    pendingStates.add(state);
    endReadableNTScheduled++;
    return originalNextTick(function endReadableNTWrapper() {
      pendingStates.delete(state);
      return callback(...args);
    });
  }
  return originalNextTick(callback, ...args);
};

// Short-lived flowing pipeline: the shape that used to schedule
// duplicate endReadableNT ticks.
const source = new PassThrough({ objectMode: true });
const transform = new Transform({
  objectMode: true,
  transform: common.mustCall((chunk, encoding, callback) => {
    callback(null, chunk);
  }, 4),
});

source.pipe(transform);
transform.on('data', common.mustCall(4));
transform.on('end', common.mustCall());
source.on('end', common.mustCall());

for (let i = 0; i < 4; i++) source.write({ i });
source.end();

process.on('exit', () => {
  process.nextTick = originalNextTick;
  assert.strictEqual(pendingStates.size, 0);
  // Both streams in the pipeline must have ended (one endReadableNT
  // tick each, at minimum).
  assert.ok(
    endReadableNTScheduled >= 2,
    `expected endReadableNT to run for both streams, saw ${
      endReadableNTScheduled}`
  );
});
