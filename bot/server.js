
const express = require("express");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const API = "https://api.dexscreener.com";

// CONFIGURATION CART3R
const MAX_POOLS = 30;
const TOKENS_PER_SCAN = 5;
const SCAN_INTERVAL = 180000;
const REQUEST_GAP = 2000;
const COOLDOWN_MS = 90000;

const MIN_LIQUIDITY = 3000;
const MIN_VOLUME_1H = 500;
const MAX_POOL_AGE = 30 * 60 * 1000;

let running = true;
let scanning = false;

let pools = new Map();
let snapshots = new Map();

let lastUpdate = null;
let lastError = null;
let scannerState = "STARTING";
let nextScanAt = Date.now();
let cooldownUntil = 0;
let scanTimer = null;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function num(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

async function apiGet(url) {
  const response = await fetch(url, {
    headers: { Accept: "application/json" },
    signal: AbortSignal.timeout(15000)
  });

  if (response.status === 429) {
    cooldownUntil = Date.now() + COOLDOWN_MS;
    throw new Error("Limite API : pause de 90 secondes");
  }

  if (!response.ok) {
    throw new Error("Erreur DEX Screener HTTP " + response.status);
  }

  return response.json();
}

function calculateScore(pool, previous) {
  const liquidity = num(pool.liquidity?.usd);
  const volume1h = num(pool.volume?.h1);
  const volume5m = num(pool.volume?.m5);

  const buys = num(pool.txns?.h1?.buys);
  const sells = num(pool.txns?.h1?.sells);
  const buys5m = num(pool.txns?.m5?.buys);
  const sells5m = num(pool.txns?.m5?.sells);

  const change = num(pool.priceChange?.h1);
  const total = buys + sells;
  const total5m = buys5m + sells5m;

  // Accélération du volume : 25 points
  let acceleration = 0;

  if (previous && previous.volume5m > 0) {
    const growth =
      (volume5m - previous.volume5m) / previous.volume5m;

    acceleration = Math.max(
      0,
      Math.min(25, 12.5 + growth * 12.5)
    );
  } else {
    acceleration = Math.min(
      25,
      (volume5m / Math.max(volume1h, 1)) * 300
    );
  }

  // Nombre d'achats : 20 points
  const buyScore = Math.min(20, buys * 0.4);

  // Équilibre achats / ventes : 15 points
  const ratioScore = total > 0
    ? (buys / total) * 15
    : 0;

  // Liquidité : 15 points
  const liquidityScore = Math.min(
    15,
    Math.log10(Math.max(1, liquidity / 1000)) * 7
  );

  // Momentum : 15 points
  const momentumScore = Math.max(
    0,
    Math.min(15, change * 1.5)
  );

  // Activité récente : 10 points
  const activityScore = Math.min(10, total5m * 0.5);

  const score = Math.round(
    acceleration +
    buyScore +
    ratioScore +
    liquidityScore +
    momentumScore +
    activityScore
  );

  return {
    score: Math.min(100, Math.max(0, score)),
    acceleration: Math.round(acceleration),
    buyRatio: Math.round(
      total > 0 ? (buys / total) * 100 : 0
    ),
    activity5m: total5m
  };
}

function formatPool(pair) {
  return {
    name: pair.baseToken?.name || "Inconnu",
    symbol: pair.baseToken?.symbol || "???",
    address: pair.baseToken?.address || "",
    pairAddress: pair.pairAddress || "",
    dex: pair.dexId || "Inconnu",

    priceUsd: num(pair.priceUsd),
    liquidity: num(pair.liquidity?.usd),
    volume1h: num(pair.volume?.h1),
    volume5m: num(pair.volume?.m5),
    volume24h: num(pair.volume?.h24),

    buys1h: num(pair.txns?.h1?.buys),
    sells1h: num(pair.txns?.h1?.sells),
    buys5m: num(pair.txns?.m5?.buys),
    sells5m: num(pair.txns?.m5?.sells),

    priceChange1h: num(pair.priceChange?.h1),
    url: pair.url || "",
    lastSeen: Date.now()
  };
}

async function scanPools() {
  if (!running || scanning) return;

  if (Date.now() < cooldownUntil) {
    scannerState = "COOLDOWN";
    nextScanAt = cooldownUntil;
    return;
  }

  scanning = true;
  scannerState = "SCANNING";

  try {
    // Une seule requête pour découvrir les tokens
    const profiles = await apiGet(
      `${API}/token-profiles/latest/v1`
    );

    const tokens = (
      Array.isArray(profiles) ? profiles : []
    )
      .filter(item =>
        item.chainId === "solana" && item.tokenAddress
      )
      .slice(0, TOKENS_PER_SCAN);

    let successful = 0;

    // Requêtes séquentielles pour limiter la pression API
    for (const token of tokens) {
      if (!running) break;

      if (Date.now() < cooldownUntil) {
        throw new Error("Limite API : pause de 90 secondes");
      }

      const data = await apiGet(
        `${API}/token-pairs/v1/solana/${token.tokenAddress}`
      );

      successful++;

      if (Array.isArray(data)) {
        for (const pair of data) {
          if (
            pair.chainId !== "solana" ||
            !pair.pairAddress
          ) continue;

          const formatted = formatPool(pair);

          if (formatted.liquidity <= 0) continue;

          const oldSnapshot = snapshots.get(
            formatted.pairAddress
          );

          const score = calculateScore(pair, oldSnapshot);

          formatted.score = score.score;
          formatted.acceleration = score.acceleration;
          formatted.buyRatio = score.buyRatio;
          formatted.activity5m = score.activity5m;

          formatted.eligible =
            formatted.liquidity >= MIN_LIQUIDITY &&
            formatted.volume1h >= MIN_VOLUME_1H;

          formatted.risk = !formatted.eligible
            ? "CRITÈRES INSUFFISANTS"
            : "À ANALYSER";

          pools.set(formatted.pairAddress, formatted);

          snapshots.set(formatted.pairAddress, {
            volume5m: formatted.volume5m,
            time: Date.now()
          });
        }
      }

      await sleep(REQUEST_GAP);
    }

    // Nettoyage des pools trop anciens
    const cutoff = Date.now() - MAX_POOL_AGE;

    for (const [key, pool] of pools.entries()) {
      if (pool.lastSeen < cutoff) {
        pools.delete(key);
        snapshots.delete(key);
      }
    }

    // Classement des 30 pools les plus actifs
    const ranked = Array.from(pools.values())
      .filter(pool => pool.eligible)
      .sort((a, b) => b.score - a.score)
      .slice(0, MAX_POOLS);

    pools = new Map(
      ranked.map(pool => [pool.pairAddress, pool])
    );

    lastUpdate = new Date().toISOString();
    lastError = null;
    scannerState = successful > 0 ? "OK" : "EMPTY";

  } catch (error) {
    lastError = error.message || "Erreur inconnue";

    scannerState = Date.now() < cooldownUntil
      ? "COOLDOWN"
      : "ERROR";

    // Les pools déjà récupérés sont conservés
  } finally {
    scanning = false;

    nextScanAt = Math.max(
      Date.now() + SCAN_INTERVAL,
      cooldownUntil
    );
  }
}

function startScanner() {
  if (scanTimer) clearInterval(scanTimer);

  running = true;
  nextScanAt = Date.now();

  scanPools();

  scanTimer = setInterval(() => {
    if (running) scanPools();
  }, SCAN_INTERVAL);
}

function stopScanner() {
  running = false;

  if (scanTimer) {
    clearInterval(scanTimer);
    scanTimer = null;
  }

  scannerState = "STOPPED";
}

app.get("/", (req, res) => {
  res.json({
    name: "CART3R",
    mode: "TEST",
    source: "DEX Screener",
    maxPools: MAX_POOLS,
    scanIntervalSeconds: SCAN_INTERVAL / 1000,
    message: "Scanner actif"
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    name: "CART3R",
    mode: "TEST",
    time: new Date().toISOString()
  });
});

app.get("/status", (req, res) => {
  res.json({
    running,
    mode: "TEST",
    realTrading: false,
    scanner: scannerState,
    poolsFound: pools.size,
    lastUpdate,
    lastError,
    scanning,
    nextScanInSeconds: Math.max(
      0,
      Math.ceil((nextScanAt - Date.now()) / 1000)
    )
  });
});

app.get("/pools", (req, res) => {
  res.json({
    mode: "TEST",
    source: "DEX Screener",
    count: pools.size,
    maxPools: MAX_POOLS,
    lastUpdate,
    pools: Array.from(pools.values())
  });
});

app.post("/test/start", (req, res) => {
  if (!running) startScanner();

  res.json({
    ok: true,
    running,
    mode: "TEST"
  });
});

app.post("/test/stop", (req, res) => {
  stopScanner();

  res.json({
    ok: true,
    running,
    mode: "TEST"
  });
});

app.listen(PORT, () => {
  console.log(
    `CART3R TEST scanner listening on port ${PORT}`
  );

  startScanner();
});
