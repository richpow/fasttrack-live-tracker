import pkgConnector from 'tiktok-live-connector';
const { WebcastPushConnection } = pkgConnector;

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const active = new Map();

const MAX_RETRIES = 2;
const CONNECT_TIMEOUT = 5000;
const BATCH_SIZE = 25;
const BATCH_DELAY = 30000 / Math.ceil(1649 / 25) > 0 ? 300 : 300;

console.log("STARTED");

process.on('unhandledRejection', (err) => {
  console.error('UNHANDLED REJECTION:', err);
});

process.on('uncaughtException', (err) => {
  console.error('UNCAUGHT EXCEPTION:', err);
});

async function getCreators() {
  const res = await pool.query(`
    SELECT username
    FROM users
    WHERE username IS NOT NULL
      AND agency_status != 'Quit'
  `);

  return res.rows.map(row => row.username);
}

async function tryConnect(username) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const conn = new WebcastPushConnection(username);

    try {
      const connectPromise = conn.connect();

      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error("timeout")), CONNECT_TIMEOUT);
      });

      await Promise.race([connectPromise, timeoutPromise]);
      return conn;
    } catch (err) {
      try {
        conn.disconnect();
      } catch {}
    }
  }

  return null;
}

async function track(username) {
  if (active.has(username)) return;

  const conn = await tryConnect(username);
  if (!conn) return;

  console.log("LIVE:", username);

  const sessionRes = await pool.query(
    "INSERT INTO live_sessions (username) VALUES ($1) RETURNING id",
    [username]
  );

  const sessionId = sessionRes.rows[0].id;

  conn.on("gift", async (data) => {
    try {
      const repeatCount = Number(data?.repeatCount || 1);
      const diamondCount = Number(data?.diamondCount || 0);
      const totalDiamonds = diamondCount * repeatCount;

      await pool.query(
        `INSERT INTO live_gift_events
         (session_id, username, gifter_username, gifter_display_name, gift_name, total_diamonds)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          sessionId,
          username,
          data?.uniqueId || null,
          data?.nickname || null,
          data?.giftName || null,
          totalDiamonds
        ]
      );
    } catch (err) {
      console.error("GIFT INSERT ERROR:", username, err);
    }
  });

  conn.on("disconnected", async () => {
    try {
      console.log("ENDED:", username);

      await pool.query(
        "UPDATE live_sessions SET ended_at = NOW() WHERE id = $1",
        [sessionId]
      );
    } catch (err) {
      console.error("END SESSION ERROR:", username, err);
    } finally {
      active.delete(username);
    }
  });

  active.set(username, conn);
}

async function poll() {
  console.log("Polling...");

  try {
    const creators = await getCreators();

    for (let i = 0; i < creators.length; i += BATCH_SIZE) {
      const batch = creators.slice(i, i + BATCH_SIZE);
      await Promise.all(batch.map(username => track(username)));
      await new Promise(resolve => setTimeout(resolve, BATCH_DELAY));
    }
  } catch (err) {
    console.error("POLL ERROR:", err);
  }
}

setInterval(poll, 30000);
poll();
