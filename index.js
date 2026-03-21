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
const BATCH_DELAY = 300;

console.log("STARTED");

process.on("unhandledRejection", (err) => {
  console.error("UNHANDLED REJECTION:", err);
});

process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
});

async function getCreators() {
  const res = await pool.query(`
    SELECT username
    FROM users
    WHERE username IS NOT NULL
      AND agency_status != 'Quit'
  `);

  return res.rows.map((row) => row.username);
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
  if (active.has(username)) {
    return "already_active";
  }

  const conn = await tryConnect(username);
  if (!conn) {
    return "not_live_or_failed";
  }

  const sessionRes = await pool.query(
    "INSERT INTO live_sessions (username) VALUES ($1) RETURNING id",
    [username]
  );

  const sessionId = sessionRes.rows[0].id;

  conn.on("gift", async (data) => {
    try {
      const giftCount = Number(data?.repeatCount || 1);
      const diamondCount = Number(data?.diamondCount || 0);
      const totalDiamonds = diamondCount * giftCount;

      await pool.query(
        `INSERT INTO live_gift_events
         (session_id, username, gifter_username, gifter_display_name, gift_name, gift_count, total_diamonds)
         VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [
          sessionId,
          username,
          data?.uniqueId || null,
          data?.nickname || null,
          data?.giftName || null,
          giftCount,
          totalDiamonds
        ]
      );
    } catch (err) {
      console.error("GIFT INSERT ERROR:", username, err);
    }
  });

  conn.on("disconnected", async () => {
    try {
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
  return "connected";
}

async function poll() {
  try {
    const creators = await getCreators();

    let alreadyActiveCount = 0;
    let connectedCount = 0;
    let failedOrOfflineCount = 0;

    for (let i = 0; i < creators.length; i += BATCH_SIZE) {
      const batch = creators.slice(i, i + BATCH_SIZE);
      const results = await Promise.all(batch.map((username) => track(username)));

      for (const result of results) {
        if (result === "already_active") alreadyActiveCount++;
        else if (result === "connected") connectedCount++;
        else failedOrOfflineCount++;
      }

      await new Promise((resolve) => setTimeout(resolve, BATCH_DELAY));
    }

    console.log(
      `POLL SUMMARY checked=${creators.length} active=${active.size} newly_connected=${connectedCount} already_active=${alreadyActiveCount} offline_or_failed=${failedOrOfflineCount}`
    );
  } catch (err) {
    console.error("POLL ERROR:", err);
  }
}

setInterval(poll, 30000);
poll();
