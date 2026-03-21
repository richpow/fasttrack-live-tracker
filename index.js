import pkgConnector from 'tiktok-live-connector';
const { TikTokLiveConnection, WebcastEvent } = pkgConnector;

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const active = new Map();

console.log("STARTED");

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

// MAIN TRACK FUNCTION (NO SEPARATE isLive)
async function tryTrack(username) {
  if (active.has(username)) return;

  const conn = new TikTokLiveConnection(username);

  try {
    // Attempt to connect → THIS is the live check
    await conn.connect();

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

  } catch {
    // Not live → silently ignore
  }
}

// POLLING WITH CONTROLLED CONCURRENCY
async function poll() {
  console.log("Polling...");

  try {
    const creators = await getCreators();

    // Limit parallel attempts to avoid overload
    const chunkSize = 50;

    for (let i = 0; i < creators.length; i += chunkSize) {
      const chunk = creators.slice(i, i + chunkSize);

      await Promise.all(
        chunk.map(username => tryTrack(username))
      );
    }

  } catch (err) {
    console.error("POLL ERROR:", err);
  }
}

// RUN
setInterval(poll, 30000);
poll();
