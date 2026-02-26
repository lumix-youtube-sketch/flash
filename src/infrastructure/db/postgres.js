const { Pool } = require('pg');

function toPg(sql) {
  let i = 0;
  return sql.replace(/\?/g, () => `$${++i}`);
}

function createPostgresDb(connectionString) {
  const pool = new Pool({ connectionString, max: 20, idleTimeoutMillis: 30000 });

  return {
    async get(sql, ...params) {
      const res = await pool.query(toPg(sql), params);
      return res.rows[0] || null;
    },
    async all(sql, ...params) {
      const res = await pool.query(toPg(sql), params);
      return res.rows;
    },
    async run(sql, ...params) {
      const res = await pool.query(toPg(sql), params);
      return { changes: res.rowCount || 0 };
    },
    async exec(sql) {
      await pool.query(sql);
    },
    async close() {
      await pool.end();
    }
  };
}

module.exports = { createPostgresDb };
