
const express = require("express");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const API = "https://api.dexscreener.com";

const MAX_POOLS = 100;
const MAX_TOKENS_PER_SCAN = 30;
const SCAN_INTERVAL = 60000;
const MIN_LIQUIDITY = 3000;
const MIN_VOLUME_1H = 500;

let running = true;
let scanning = false;
let pools = [];
let lastUpdate = null;
let lastError = null;
let scannerState = "STARTING";
let nextScanAt = Date.now();
let cooldownUntil = 0;
let scanTimer = null;

function number(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

async function apiGet(url) {
  const response = await fetch(url, {
    headers: { Accept: "application/json" },
    signal: AbortSignal.timeout(15000)
  });

  if (response.status === 429) {
    cooldownUntil = Date.now() + 60000;
    throw new Error("Limite API : pause de 60 secondes");
  }

  if (!response.ok) {
    throw new Error("Erreur API DEX Screener : HTTP " + response.status);
  }

  return response.json();
}

function calculateScore(pool) {
  const liquidity = number(pool.liquidity?.usd);
  const volume = number(pool.volume?.h1);
  const buys = number(pool.txns?.h1?.buys);
  const sells = number(pool.txns?.h1?.sells);
  const change = number(pool.priceChange?.h1);

  let score = 0;

  // Liquidité : maximum 25 points
  score += Math.min(25, Math.log10(Math.max(1, liquidity)) * 5);

  // Volume : maximum 25 points
  score += Math.min(25, Math.log10(Math.max(1, volume)) * 5);

  // Activité des transactions : maximum 20 points
  score += Math.min(20, (buys + sells) * 0.4);

  // Équilibre acheteurs / vendeurs : maximum 15 points
  if (buys + sells > 0) {
    score += Math.min(15, (buys / (buys + sells)) * 15);
  }

  // Momentum positif : maximum 15 points
  if (change > 0) {
    score += Math.min(15, change * 1.5);
  }

  return Math.max(0, Math.min(100, Math.round(score)));
}

function formatPool(pool) {
  const liquidity = number(pool.liquidity?.usd);
  const volume = number(pool.volume?.h1);
  const buys = number(pool.txns?.h1?.buys);
  const sells = number(pool.txns?.h1?.sells);
  const change = number(pool.priceChange?.h1);

  return {
    name: pool.baseToken?.name || "Inconnu",
    symbol: pool.baseToken?.symbol || "???",
    address: pool.baseToken?.address || "",
    pairAddress: pool.pairAddress || "",
    dex: pool.dexId || "Inconnu",
    priceUsd: number(pool.priceUsd),
    liquidity,
    volume1h: volume,
    volume24h: number(pool.volume?.h24),
    buys1h: buys,
    sells1h: sells,
    priceChange1h: change,
    score: calculateScore(pool),
    url: pool.url || "",
    eligible: liquidity >= MIN_LIQUIDITY && volume >= MIN_VOLUME_1H,
    risk: liquidity < MIN_LIQUIDITY ? "LIQUIDITÉ FAIBLE" : "À ANALYSER"
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
  lastError = null;

  try {
    // Découverte des tokens récemment publiés
    const profiles = await apiGet(
      `${API}/token-profiles/latest/v1`
    );

    const tokens = (Array.isArray(profiles) ? profiles : [])
      .filter(item => item.chainId === "solana" && item.tokenAddress)
      .slice(0, MAX_TOKENS_PER_SCAN);

    const found = new Map();
    let index = 0;

    // Traitement limité à 5 requêtes simultanées
    async function worker() {
      while (index < tokens.length) {
        const token = tokens[index++];

        try {
          const data = await apiGet(
            `${API}/token-pairs/v1/solana/${token.tokenAddress}`
          );

          if (!Array.isArray(data)) continue;

          for (const pair of data) {
            if (pair.chainId !== "solana" || !pair.pairAddress) continue;

            const formatted = formatPool(pair);

            if (formatted.liquidity <= 0) continue;

            found.set(formatted.pairAddress, formatted);
          }
        } catch (error) {
          if (String(error.message).includes("Limite API")) {
            throw error;
          }
        }
      }
    }

    await Promise.all(
      Array.from({ length: 5 }, () => worker())
    );

    pools = Array.from(found.values())
      .sort((a, b) => b.score - a.score)
      .slice(0, MAX_POOLS);

    lastUpdate = new Date().toISOString();
    lastError = null;
    scannerState = "OK";

  } catch (error) {
    lastError = error.message || "Erreur inconnue";
    scannerState = "ERROR";
  } finally {
    scanning = false;
    nextScanAt = Date.now() + SCAN_INTERVAL;
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
    message: "Scanner DEX Screener actif",
    endpoints: ["/health", "/status", "/pools"]
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
    poolsFound: pools.length,
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
    count: pools.length,
    lastUpdate,
    pools
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
  console.log(`CART3R DEX SCANNER TEST running on port ${PORT}`);
  startScanner();
});
