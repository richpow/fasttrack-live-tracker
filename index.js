import pkgConnector from 'tiktok-live-connector';
const { TikTokLiveConnection, WebcastEvent } = pkgConnector;

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const active = new Map();

console.log("🚀 App started");

// GET CREATORS
async function getCreators() {
  const res = await pool.query(`
    SELECT username 
    FROM users 
    WHERE username IS NOT NULL
    AND agency_status != 'Quit'
  `);
  console.log(`Loaded ${res.rowCount} creators`);
  return res.rows.map(r => r.username);
}

// CHECK IF LIVE
async function isLive(username) {
  try {
    const conn = new TikTokLiveConnection(username);
    const info = await conn.getRoomInfo();
    return info?.status === 2;
  } catch {
    return false;
  }
}

// START TRACKING
async function track(username) {
  if (active.has(username)) return;

  console.log("🔴 LIVE:", username);

  const session = await pool.query(
    "INSERT INTO live_sessions (username) VALUES ($1) RETURNING id",
    [username]
  );

  const sessionId = session.rows[0].id;

  const conn = new TikTokLiveConnection(username);

  conn.on(WebcastEvent.GIFT, async (data) => {
    const total = data.diamondCount * (data.repeatCount || 1);

    console.log(`🎁 ${username} received ${data.giftName} x${data.repeatCount || 1}`);

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
    console.log("⚫ ENDED:", username);

    await pool.query(
      "UPDATE live_sessions SET ended_at = NOW() WHERE id = $1",
      [sessionId]
    );

    active.delete(username);
  });

  await conn.connect();
  active.set(username, conn);
}

// POLL LOOP
async function poll() {
  console.log("⏱ Polling...");

  const creators = await getCreators();

  for (const username of creators) {
    if (active.has(username)) continue;

    const live = await isLive(username);
    if (live) await track(username);
  }
}

setInterval(poll, 30000);

// RUN IMMEDIATELY
poll();
