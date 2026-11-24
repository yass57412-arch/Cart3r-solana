
const express = require("express");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json({ limit: "10kb" }));

const API = "https://api.geckoterminal.com/api/v2";
const MAX_PAGES = 5;
const PAGE_GAP = 8000;
const SCAN_INTERVAL = 180000;

let status = {
  running: true,
  mode: "TEST",
  realTrading: false,
  scanner: "STARTING",
  poolsFound: 0,
  lastUpdate: null,
  lastError: null
};

let pools = [];
let scanning = false;
let cooldownUntil = 0;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function num(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function calculateScore(liquidity, volume, change) {
  const l = Math.min(30, Math.log10(liquidity + 1) * 6);
  const v = Math.min(35, Math.log10(volume + 1) * 7);
  const m = Math.max(0, Math.min(35, 17.5 + change * 1.75));
  return Math.round(l + v + m);
}

async function scanPools() {
  if (!status.running || scanning) return;

  if (Date.now() < cooldownUntil) {
    status.scanner = "API_COOLDOWN";
    return;
  }

  scanning = true;
  status.scanner = "SCANNING";
  status.lastError = null;

  const collected = new Map();

  try {
    for (let page = 1; page <= MAX_PAGES; page++) {
      if (!status.running) break;

      if (page > 1) await sleep(PAGE_GAP);

      const url =
        `${API}/networks/solana/new_pools` +
        `?page=${page}&include=base_token`;

      const response = await fetch(url, {
        headers: {
          Accept: "application/json;version=20230302"
        },
        signal: AbortSignal.timeout(20000)
      });

      if (response.status === 429) {
        cooldownUntil = Date.now() + 60000;
        throw new Error("Limite API : pause de 60 secondes");
      }

      if (!response.ok) {
        throw new Error(`GeckoTerminal HTTP ${response.status}`);
      }

      const json = await response.json();

      const included = new Map(
        (json.included || []).map(item => [
          item.id,
          item.attributes || {}
        ])
      );

      for (const pool of json.data || []) {
        const a = pool.attributes || {};
        const baseId = pool.relationships?.base_token?.data?.id;
        const token = included.get(baseId) || {};

        const liquidity = num(a.reserve_in_usd);
        const volume = num(a.volume_usd?.h24);
        const change = num(a.price_change_percentage?.h24);

        const item = {
          id: pool.id,
          address: String(pool.id || "").replace(/^solana_/, ""),
          name: token.name || "Token inconnu",
          symbol: token.symbol || "???",
          priceUsd: num(a.base_token_price_usd),
          liquidityUsd: liquidity,
          volume24hUsd: volume,
          change24hPct: change,
          buys24h: num(a.transactions?.h24?.buys),
          sells24h: num(a.transactions?.h24?.sells),
          createdAt: a.pool_created_at || null,
          score: calculateScore(liquidity, volume, change)
        };

        collected.set(item.id, item);
      }
    }

    if (status.running && collected.size > 0) {
      pools = [...collected.values()]
        .sort((a, b) => b.score - a.score)
        .slice(0, 100);

      status.poolsFound = pools.length;
      status.lastUpdate = new Date().toISOString();
      status.scanner = "OK";
      status.lastError = null;
    } else if (status.running) {
      status.scanner = "NO_DATA";
    }

  } catch (error) {
    status.scanner = "ERROR";
    status.lastError = error.message;
    console.error("CART3R scanner:", error.message);

  } finally {
    scanning = false;
  }
}

// Page d'accueil
app.get("/", (req, res) => {
  res.json({
    name: "CART3R",
    mode: "TEST",
    message: "CART3R scanner server is online",
    endpoints: ["/health", "/status", "/pools"]
  });
});

// Vérification serveur
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    name: "CART3R",
    mode: "TEST",
    time: new Date().toISOString()
  });
});

// État du scanner
app.get("/status", (req, res) => {
  res.json({
    ...status,
    scanning,
    nextScanInSeconds: Math.max(
      0,
      Math.ceil((cooldownUntil - Date.now()) / 1000)
    )
  });
});

// Liste des pools
app.get("/pools", (req, res) => {
  res.json({
    mode: "TEST",
    count: pools.length,
    lastUpdate: status.lastUpdate,
    pools
  });
});

// Redémarrage du scanner
app.post("/test/start", (req, res) => {
  status.running = true;
  status.scanner = "WAITING";
  scanPools();

  res.json({
    ok: true,
    message: "Scanner de test démarré"
  });
});

// Arrêt du scanner
app.post("/test/stop", (req, res) => {
  status.running = false;
  status.scanner = "STOPPED";

  res.json({
    ok: true,
    message: "Scanner arrêté"
  });
});

app.listen(PORT, () => {
  console.log(`CART3R TEST server listening on ${PORT}`);

  scanPools();
  setInterval(scanPools, SCAN_INTERVAL);
});
