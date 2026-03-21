import pkgConnector from 'tiktok-live-connector';
const { WebcastPushConnection, WebcastEvent } = pkgConnector;

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const active = new Map();

console.log("STARTED");

// CONFIG
const MAX_RETRIES = 2;
const CONNECT_TIMEOUT = 5000;
const BATCH_SIZE = 25;
const BATCH_DELAY = 300;

// GET CREATORS (EXCLUDES QUIT)
async function getCreators() {
  const res = await pool.query(`
    SELECT username 
    FROM users 
    WHERE username IS NOT NULL
    AND agency_status != 'Quit'
  `);
  return res.rows.map(r => r.username);
}

// TRY CONNECT WITH RETRY
async function tryConnect(username) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const conn = new WebcastPushConnection(username);

    try {
      const connectPromise = conn.connect();

      const timeoutPromise = new Promise((_, reject) =>
        setTimeout(() => reject(new Error("timeout")), CONNECT_TIMEOUT)
      );

      await Promise.race([connectPromise, timeoutPromise]);

      return conn; // success

    } catch {
      try { conn.disconnect(); } catch {}
    }
  }
  return null;
}

// TRACK
async function track(username) {
  if (active.has(username)) return;

  const conn = await tryConnect(username);
  if (!conn) return;

  console.log("LIVE:", username);

  const session = await pool.query(
    "INSERT INTO live_sessions (username) VALUES ($1) RETURNING id",
    [username]
  );

  const sessionId = session.rows[0].id;

  conn.on(WebcastEvent.GIFT, async (data) => {
    const total = data.diamondCount * (data.repeatCount || 1);

    await pool.query(
      `INSERT INTO live_gift_events 
      (session_id, username, gifter_username, gifter_display_name, gift_name, total_diamonds) 
      VALUES ($1,$2,$3,$4,$5,$6)`,
      [
        sessionId,
        username,
        data.uniqueId,
        data.nickname,
        data.giftName,
        total
      ]
    );
  });

  conn.on("disconnected", async () => {
    console.log("ENDED:", username);

    await pool.query(
      "UPDATE live_sessions SET ended_at = NOW() WHERE id = $1",
      [sessionId]
    );

    active.delete(username);
  });

  active.set(username, conn);
}

// POLL
async function poll() {
  console.log("Polling...");

  try {
    const creators = await getCreators();

    for (let i = 0; i < creators.length; i += BATCH_SIZE) {
      const batch = creators.slice(i, i + BATCH_SIZE);

      await Promise.all(batch.map(username => track(username)));

      await new Promise(r => setTimeout(r, BATCH_DELAY));
    }

  } catch (err) {
    console.error("POLL ERROR:", err);
  }
}

// RUN
setInterval(poll, 30000);
poll();
