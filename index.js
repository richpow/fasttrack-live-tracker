import pkgConnector from "@adamjessop/tiktok-live-connector";
const { TikTokLiveConnection } = pkgConnector;

import pkg from "pg";
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5
});

const active = new Map();
const pending = new Set();

const POLL_INTERVAL_SECONDS = Number(process.env.POLL_INTERVAL_SECONDS || 45);
const CONCURRENCY = Number(process.env.CONCURRENCY || 10);
const OFFLINE_MISS_THRESHOLD = Number(process.env.OFFLINE_MISS_THRESHOLD || 3);

console.log("STARTED");

process.on("unhandledRejection", (err) => {
  console.error("UNHANDLED REJECTION:", err);
});

process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
});

function shuffle(array) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
}

async function runWithConcurrency(items, limit, fn) {
  let index = 0;

  async function worker() {
    while (true) {
      if (index >= items.length) return;
      const item = items[index++];
      await fn(item);
    }
  }

  const workers = [];
  for (let i = 0; i < limit; i++) {
    workers.push(worker());
  }

  await Promise.all(workers);
}

async function getCreators() {
  const res = await pool.query(`
    SELECT username
    FROM users
    WHERE username IS NOT NULL
      AND username <> ''
      AND agency_status != 'Quit'
  `);

  return res.rows.map((row) => String(row.username).replace(/^@/, "").trim());
}

async function insertSessionIfNeeded(username) {
  const existing = await pool.query(
    `
    SELECT id
    FROM live_sessions
    WHERE username = $1
      AND ended_at IS NULL
    ORDER BY id DESC
    LIMIT 1
    `,
    [username]
  );

  if (existing.rows.length) {
    return existing.rows[0].id;
  }

  const created = await pool.query(
    `
    INSERT INTO live_sessions (username, started_at)
    VALUES ($1, NOW())
    RETURNING id
    `,
    [username]
  );

  return created.rows[0].id;
}

async function closeOpenSession(username) {
  await pool.query(
    `
    UPDATE live_sessions
    SET ended_at = NOW()
    WHERE username = $1
      AND ended_at IS NULL
    `,
    [username]
  );
}

async function insertGift(sessionId, username, data) {
  const giftCount = Number(data?.repeatCount || 1);
  const diamondCount = Number(data?.diamondCount || 0);
  const totalDiamonds = giftCount * diamondCount;

  await pool.query(
    `
    INSERT INTO live_gift_events
    (
      session_id,
      username,
      gifter_username,
      gifter_display_name,
      gift_name,
      gift_count,
      total_diamonds,
      created_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
    `,
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
}

async function fetchLiveStatus(username) {
  const conn = new TikTokLiveConnection(username, {
    processInitialData: false,
    fetchRoomInfoOnConnect: true
  });

  try {
    return Boolean(await conn.fetchIsLive());
  } catch {
    return false;
  }
}

async function startWatcher(username) {
  if (active.has(username) || pending.has(username)) {
    return "already_active";
  }

  pending.add(username);

  try {
    const sessionId = await insertSessionIfNeeded(username);

    const watcher = new TikTokLiveConnection(username, {
      processInitialData: false,
      fetchRoomInfoOnConnect: true
    });

    let ended = false;
    let missCount = 0;

    function finish(reason) {
      if (ended) return;
      ended = true;

      active.delete(username);
      pending.delete(username);

      closeOpenSession(username).catch((err) => {
        console.error("END SESSION ERROR:", username, err);
      });
    }

    watcher.on("gift", async (data) => {
      try {
        await insertGift(sessionId, username, data);
      } catch (err) {
        console.error("GIFT INSERT ERROR:", username, err);
      }
    });

    watcher.on("disconnected", async () => {
      missCount += 1;

      if (missCount >= OFFLINE_MISS_THRESHOLD) {
        finish("disconnected");
        return;
      }

      try {
        await watcher.connect();
      } catch {
        if (missCount >= OFFLINE_MISS_THRESHOLD) {
          finish("reconnect_failed");
        }
      }
    });

    await watcher.connect();

    active.set(username, {
      connection: watcher,
      sessionId
    });

    pending.delete(username);
    return "connected";
  } catch {
    pending.delete(username);
    return "not_live_or_failed";
  }
}

async function poll() {
  try {
    const creators = await getCreators();
    shuffle(creators);

    let alreadyActiveCount = 0;
    let connectedCount = 0;
    let offlineCount = 0;

    await runWithConcurrency(creators, CONCURRENCY, async (username) => {
      if (active.has(username) || pending.has(username)) {
        alreadyActiveCount += 1;
        return;
      }

      const live = await fetchLiveStatus(username);

      if (!live) {
        offlineCount += 1;
        return;
      }

      const result = await startWatcher(username);

      if (result === "connected") {
        connectedCount += 1;
      } else if (result === "already_active") {
        alreadyActiveCount += 1;
      } else {
        offlineCount += 1;
      }
    });

    console.log(
      `POLL SUMMARY checked=${creators.length} active=${active.size} pending=${pending.size} newly_connected=${connectedCount} already_active=${alreadyActiveCount} offline=${offlineCount}`
    );
  } catch (err) {
    console.error("POLL ERROR:", err);
  }
}

setInterval(() => {
  poll().catch((err) => {
    console.error("POLL LOOP ERROR:", err);
  });
}, POLL_INTERVAL_SECONDS * 1000);

poll().catch((err) => {
  console.error("INITIAL POLL ERROR:", err);
});
