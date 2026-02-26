function setupGracefulShutdown({ logger, bot, db, telegramDispatcher, cronTasks = [] }) {
  let shuttingDown = false;

  async function shutdown(signal) {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info(`Received ${signal}, starting graceful shutdown`);

    for (const task of cronTasks) {
      try { task.stop(); } catch (_) {}
    }

    try { await telegramDispatcher.shutdown(); } catch (e) { logger.warn(`Queue shutdown error: ${e.message}`); }
    try { bot.stop(signal); } catch (e) { logger.warn(`Bot stop error: ${e.message}`); }
    try { await db.close(); } catch (e) { logger.warn(`DB close error: ${e.message}`); }

    process.exit(0);
  }

  process.once('SIGINT', () => shutdown('SIGINT'));
  process.once('SIGTERM', () => shutdown('SIGTERM'));
}

module.exports = { setupGracefulShutdown };
