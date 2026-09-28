import express from "express";
import pg from "pg";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const pool = new pg.Pool({
  host: process.env.PGHOST || "postgres",
  port: Number(process.env.PGPORT || 5432),
  user: process.env.PGUSER || "demo",
  password: process.env.PGPASSWORD || "demo",
  database: process.env.PGDATABASE || "demo",
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Ждём, пока Postgres поднимется и таблицы будут созданы
async function waitForDb(retries = 30, delay = 2000) {
  for (let i = 1; i <= retries; i++) {
    try {
      const r = await pool.query(
        "SELECT to_regclass('public.orders') IS NOT NULL AS ok"
      );
      if (r.rows[0].ok) {
        console.log("[WEB] Postgres готов, таблицы найдены.");
        return;
      }
    } catch (e) {
      console.log(`[WEB] Ждём Postgres... (${i}/${retries}): ${e.message}`);
    }
    await sleep(delay);
  }
  throw new Error("Postgres не поднялся за отведённое время");
}

// ─── API: создать заказ ─────────────────────────────────────────────
// Записываем ТОЛЬКО в orders + outbox в одной транзакции — точно так же,
// как это делает фоновый OrderService в C#-producer'е.
// Relay (C#) подхватит запись и отправит в Kafka.
app.post("/api/orders", async (req, res) => {
  const { customerId, amount } = req.body || {};
  if (!amount || isNaN(Number(amount))) {
    return res.status(400).json({ error: "amount обязателен" });
  }

  const orderId = crypto.randomUUID();
  const eventId = crypto.randomUUID();
  const custId = customerId || crypto.randomUUID();
  const amt = Number(amount);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    await client.query(
      `INSERT INTO orders (id, customer_id, amount, status)
       VALUES ($1, $2, $3, 'CREATED')`,
      [orderId, custId, amt]
    );

    const payload = {
      id: eventId,
      timestamp: Math.floor(Date.now() / 1000),
      payload: { orderId, customerId: custId, amount: amt, action: "RESERVE" },
    };

    await client.query(
      `INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
       VALUES ('Order', $1, 'OrderReserveRequested', $2::jsonb)`,
      [orderId, JSON.stringify(payload)]
    );

    await client.query("COMMIT");
    console.log(`[WEB] Заказ ${orderId} создан, event ${eventId} → outbox`);
    res.json({ orderId, eventId });
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("[WEB] Ошибка создания заказа:", e);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

// ─── API: статус заказа на всех этапах ──────────────────────────────
app.get("/api/orders/:orderId", async (req, res) => {
  const { orderId } = req.params;
  try {
    const order = await pool.query(
      `SELECT id, customer_id, amount, status, created_at
       FROM orders WHERE id = $1`,
      [orderId]
    );
    if (order.rows.length === 0) {
      return res.status(404).json({ error: "Заказ не найден" });
    }

    const outbox = await pool.query(
      `SELECT id, status, created_at, published_at, payload
       FROM outbox
       WHERE aggregate_id = $1
       ORDER BY id DESC LIMIT 1`,
      [orderId]
    );

    let inbox = { rows: [] };
    if (outbox.rows.length > 0) {
      const eventId = outbox.rows[0].payload.id;
      inbox = await pool.query(
        `SELECT message_key, processed_at FROM inbox WHERE message_key = $1`,
        [eventId]
      );
    }

    res.json({
      order: order.rows[0],
      outbox: outbox.rows[0] || null,
      inbox: inbox.rows[0] || null,
    });
  } catch (e) {
    console.error("[WEB] Ошибка получения статуса:", e);
    res.status(500).json({ error: e.message });
  }
});

// ─── API: последние заказы ──────────────────────────────────────────
app.get("/api/orders", async (_req, res) => {
  const r = await pool.query(
    `SELECT id, amount, status, created_at FROM orders
     ORDER BY created_at DESC LIMIT 20`
  );
  res.json(r.rows);
});

const PORT = process.env.PORT || 3000;

await waitForDb();
app.listen(PORT, () => console.log(`[WEB] Сервер запущен: http://localhost:${PORT}`));