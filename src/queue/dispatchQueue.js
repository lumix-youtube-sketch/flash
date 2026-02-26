const { InMemoryQueue } = require('./inMemoryQueue');

function createDispatchQueue({ worker, concurrency = 1 }) {
  // Redis can be connected here later; in-memory queue is default fallback.
  return new InMemoryQueue({ worker, concurrency });
}

module.exports = { createDispatchQueue };
