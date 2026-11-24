
const express = require("express");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const DEX = "https://api.dexscreener.com";

const MAX_POOLS = 30;
const SCAN_INTERVAL = 180000;
const COOLDOWN_MS = 180000;
const MIN_LIQUIDITY = 3000;
const MIN_VOLUME_1H = 500;
const MAX_AGE = 30 * 60 * 1000;

let running = true;
let scanning = false;
let pools = new Map();
let snapshots = new Map();

let scanner = "STARTING";
let lastUpdate = null;
let lastError = null;
let cooldownUntil = 0;
let nextScanAt = Date.now();
let timer = null;
let heliusLastCheck = null;
let heliusLastSlot = null;
let heliusError = null;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

async function request(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    signal: AbortSignal.timeout(20000)
  });

  if (response.status === 429) {
    cooldownUntil = Date.now() + COOLDOWN_MS;
    throw new Error("Limite API : pause de 180 secondes");
  }

  if (!response.ok) {
    throw new Error("HTTP " + response.status);
  }

  return response.json();
}

function scorePair(pair) {
  const liquidity = num(pair.liquidity?.usd);
  const volume1h = num(pair.volume?.h1);
  const volume5m = num(pair.volume?.m5);

  const buys = num(pair.txns?.h1?.buys);
  const sells = num(pair.txns?.h1?.sells);
  const buys5m = num(pair.txns?.m5?.buys);
  const sells5m = num(pair.txns?.m5?.sells);

  const change1h = num(pair.priceChange?.h1);
  const change5m = num(pair.priceChange?.m5);

  const total = buys + sells;
  const total5m = buys5m + sells5m;

  const old = snapshots.get(pair.pairAddress);
  let acceleration = 0;

  if (old && old.volume5m > 0) {
    const growth =
      (volume5m - old.volume5m) / old.volume5m;

    acceleration = Math.max(
      0,
      Math.min(25, 12.5 + growth * 12.5)
    );
  } else {
    acceleration = Math.min(
      25,
      volume1h > 0 ? volume5m / volume1h * 300 : 0
    );
  }

  const volumeScore = Math.min(
    20,
    Math.log10(Math.max(1, volume1h)) * 5
  );

  const buyScore = Math.min(15, buys * 0.3);

  const ratioScore = total > 0
    ? (buys / total) * 15
    : 0;

  const liquidityScore = Math.min(
    15,
    Math.log10(Math.max(1, liquidity / 1000)) * 7
  );

  const momentumScore = Math.max(
    0,
    Math.min(10, change1h * 0.7 + change5m * 0.5)
  );

  const activityScore = Math.min(
    10,
    total5m * 0.5
  );

  const rawScore =
    acceleration +
    volumeScore +
    buyScore +
    ratioScore +
    liquidityScore +
    momentumScore +
    activityScore;

  const score = Math.round(
    Math.max(0, Math.min(100, rawScore))
  );

  snapshots.set(pair.pairAddress, {
    volume5m,
    time: Date.now()
  });

  return {
    score,
    acceleration: Math.round(acceleration),
    buyRatio: Math.round(
      total > 0 ? buys / total * 100 : 0
    ),
    activity5m: total5m
  };
}

function formatPair(pair) {
  const liquidity = num(pair.liquidity?.usd);
  const volume1h = num(pair.volume?.h1);
  const result = scorePair(pair);

  const created = num(pair.pairCreatedAt);
  const ageMinutes = created
    ? Math.max(0, (Date.now() - created) / 60000)
    : null;

  return {
    name: pair.baseToken?.name || "Inconnu",
    symbol: pair.baseToken?.symbol || "???",
    address: pair.baseToken?.address || "",
    pairAddress: pair.pairAddress,
    dex: pair.dexId || "Inconnu",

    priceUsd: num(pair.priceUsd),
    liquidity,
    volume1h,
    volume5m: num(pair.volume?.m5),
    volume24h: num(pair.volume?.h24),

    buys1h: num(pair.txns?.h1?.buys),
    sells1h: num(pair.txns?.h1?.sells),
    buys5m: num(pair.txns?.m5?.buys),
    sells5m: num(pair.txns?.m5?.sells),

    priceChange1h: num(pair.priceChange?.h1),
    priceChange5m: num(pair.priceChange?.m5),

    ageMinutes,
    score: result.score,
    acceleration: result.acceleration,
    buyRatio: result.buyRatio,
    activity5m: result.activity5m,

    url: pair.url || "",
    eligible:
      liquidity >= MIN_LIQUIDITY &&
      volume1h >= MIN_VOLUME_1H,

    lastSeen: Date.now()
  };
}

async function scanPools() {
  if (!running || scanning) return;

  if (Date.now() < cooldownUntil) {
    scanner = "COOLDOWN";
    nextScanAt = cooldownUntil;
    return;
  }

  scanning = true;
  scanner = "SCANNING";

  try {
    // REQUETE 1 : découverte des tokens
    const profiles = await request(
      `${DEX}/token-profiles/latest/v1`
    );

    const addresses = [
      ...new Set(
        (Array.isArray(profiles) ? profiles : [])
          .filter(p =>
            p.chainId === "solana" && p.tokenAddress
          )
          .map(p => p.tokenAddress)
      )
    ].slice(0, 30);

    if (addresses.length === 0) {
      throw new Error("Aucun token Solana découvert");
    }

    // REQUETE 2 : données groupées des 30 tokens
    const url =
      `${DEX}/tokens/v1/solana/${addresses.join(",")}`;

    const data = await request(url);

    if (!Array.isArray(data)) {
      throw new Error("Réponse DEX Screener invalide");
    }

    for (const pair of data) {
      if (
        pair.chainId !== "solana" ||
        !pair.pairAddress
      ) continue;

      const formatted = formatPair(pair);

      if (formatted.liquidity <= 0) continue;

      pools.set(formatted.pairAddress, formatted);
    }

    // Supprimer les pools non actualisés depuis 30 minutes
    const cutoff = Date.now() - MAX_AGE;

    for (const [key, pool] of pools.entries()) {
      if (pool.lastSeen < cutoff) {
        pools.delete(key);
        snapshots.delete(key);
      }
    }

    // Garder les 30 meilleures opportunités selon les critères
    const ranked = Array.from(pools.values())
      .filter(p => p.eligible)
      .sort((a, b) => b.score - a.score)
      .slice(0, MAX_POOLS);

    pools = new Map(
      ranked.map(p => [p.pairAddress, p])
    );

    lastUpdate = new Date().toISOString();
    lastError = null;
    scanner = "OK";

  } catch (error) {
    lastError = error.message || "Erreur inconnue";

    scanner = Date.now() < cooldownUntil
      ? "COOLDOWN"
      : "ERROR";

    // Ne pas effacer les pools en cas d'échec.
  } finally {
    scanning = false;
    nextScanAt = Math.max(
      Date.now() + SCAN_INTERVAL,
      cooldownUntil
    );
  }
}

// Vérification indépendante de Helius
async function checkHelius() {
  const rpc = process.env.HELIUS_RPC_URL;

  if (!rpc) {
    return {
      ok: false,
      error: "HELIUS_RPC_URL absente"
    };
  }

  try {
    const result = await request(rpc, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "getSlot",
        params: [{ commitment: "confirmed" }]
      })
    });

    if (result.error || typeof result.result !== "number") {
      throw new Error(
        result.error?.message || "Réponse RPC invalide"
      );
    }

    heliusLastSlot = result.result;
    heliusLastCheck = new Date().toISOString();
    heliusError = null;

    return {
      ok: true,
      slot: heliusLastSlot,
      checkedAt: heliusLastCheck
    };

  } catch (error) {
    heliusError = error.message;
    heliusLastCheck = new Date().toISOString();

    return {
      ok: false,
      error: heliusError,
      checkedAt: heliusLastCheck
    };
  }
}

function startScanner() {
  if (timer) clearInterval(timer);

  running = true;
  nextScanAt = Date.now();

  scanPools();

  timer = setInterval(() => {
    if (running) scanPools();
  }, SCAN_INTERVAL);
}

function stopScanner() {
  running = false;

  if (timer) {
    clearInterval(timer);
    timer = null;
  }

  scanner = "STOPPED";
}

app.get("/", (req, res) => {
  res.json({
    name: "CART3R",
    mode: "TEST",
    source: ["DEX Screener", "Helius RPC"],
    maxPools: MAX_POOLS,
    scanIntervalSeconds: SCAN_INTERVAL / 1000
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
    scanner,
    poolsFound: pools.size,
    lastUpdate,
    lastError,
    scanning,
    nextScanInSeconds: Math.max(
      0,
      Math.ceil((nextScanAt - Date.now()) / 1000)
    ),
    helius: {
      configured: Boolean(process.env.HELIUS_RPC_URL),
      lastCheck: heliusLastCheck,
      lastSlot: heliusLastSlot,
      lastError: heliusError
    }
  });
});

app.get("/helius-test", async (req, res) => {
  const result = await checkHelius();
  res.status(result.ok ? 200 : 502).json(result);
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
    `CART3R TEST engine listening on port ${PORT}`
  );

  startScanner();
});
