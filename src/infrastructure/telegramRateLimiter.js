const { createDispatchQueue } = require('../queue/dispatchQueue');

function createTelegramDispatcher({ logger, maxPerSecond = 28 }) {
  const intervalMs = Math.ceil(1000 / maxPerSecond);
  let nextAllowedTs = 0;

  async function withBackoff(fn) {
    for (let attempt = 0; attempt < 6; attempt += 1) {
      try {
        return await fn();
      } catch (error) {
        const code = error?.response?.error_code;
        const retryAfter = Number(error?.response?.parameters?.retry_after || 0);
        if (code === 429) {
          const sleepMs = Math.max((retryAfter || 1) * 1000, 1000);
          logger.warn(`Telegram 429 received; retry after ${sleepMs}ms`);
          await new Promise((resolve) => setTimeout(resolve, sleepMs));
          continue;
        }
        throw error;
      }
    }
    throw new Error('Telegram call failed after retries');
  }

  const queue = createDispatchQueue({
    concurrency: 1,
    worker: async (task) => {
      const now = Date.now();
      const waitMs = Math.max(0, nextAllowedTs - now);
      if (waitMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, waitMs));
      }
      nextAllowedTs = Date.now() + intervalMs;
      return withBackoff(task);
    }
  });

  return {
    enqueue: (task) => queue.push(task),
    shutdown: () => queue.shutdown()
  };
}

module.exports = { createTelegramDispatcher };
