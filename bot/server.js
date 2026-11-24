
"use strict";

const express = require("express");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const HELIUS_RPC_URL = process.env.HELIUS_RPC_URL || "";

const CONFIG = {
  MAX_POOLS: 30,
  SCAN_INTERVAL: 180000,
  COOLDOWN_MS: 180000,
  REQUEST_TIMEOUT: 15000,

  MIN_LIQUIDITY_USD: 3000,
  MIN_VOLUME_1H_USD: 500,
  MIN_AGE_MINUTES: 5,

  // Simulation indicative uniquement
  TEST_TRADE_USD: 10,

  DEX_PROFILES:
    "https://api.dexscreener.com/token-profiles/latest/v1",

  DEX_TOKENS:
    "https://api.dexscreener.com/tokens/v1/solana/"
};

let running = true;
let scanning = false;
let mode = "TEST";
let realTrading = false;

let scannerState = "STARTING";
let pools = [];
let lastUpdate = null;
let lastError = null;
let cooldownUntil = 0;
let nextScanInSeconds = 0;

let heliusLastSlot = null;
let heliusLastCheck = null;
let heliusError = null;

let scanTimer = null;
let countdownTimer = null;

const snapshots = new Map();

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function number(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function round(value, decimals = 2) {
  const p = Math.pow(10, decimals);
  return Math.round(number(value) * p) / p;
}

function shorten(value) {
  if (!value || typeof value !== "string") return "";
  return value.slice(0, 6) + "..." + value.slice(-4);
}

async function fetchJson(url, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    CONFIG.REQUEST_TIMEOUT
  );

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
      headers: {
        "Accept": "application/json",
        ...(options.headers || {})
      }
    });

    if (response.status === 429) {
      const error = new Error("Limite API : pause de 180 secondes");
      error.status = 429;
      throw error;
    }

    if (!response.ok) {
      throw new Error("HTTP " + response.status);
    }

    return await response.json();
  } finally {
    clearTimeout(timeout);
  }
}

// --------------------------------------------------
// HELIUS : RPC SOLANA
// --------------------------------------------------

async function heliusRequest(method, params = []) {
  if (!HELIUS_RPC_URL) {
    throw new Error("HELIUS_RPC_URL non configurée");
  }

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    CONFIG.REQUEST_TIMEOUT
  );

  try {
    const response = await fetch(HELIUS_RPC_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method,
        params
      }),
      signal: controller.signal
    });

    if (!response.ok) {
      throw new Error("Helius HTTP " + response.status);
    }

    const data = await response.json();

    if (data.error) {
      throw new Error(data.error.message || "Erreur RPC Helius");
    }

    return data.result;
  } finally {
    clearTimeout(timeout);
  }
}

async function checkHelius() {
  if (!HELIUS_RPC_URL) {
    heliusError = "Helius non configuré";
    return false;
  }

  try {
    const slot = await heliusRequest("getSlot", [
      { commitment: "confirmed" }
    ]);

    heliusLastSlot = slot;
    heliusLastCheck = new Date().toISOString();
    heliusError = null;

    return true;
  } catch (error) {
    heliusError = error.message;
    return false;
  }
}

// --------------------------------------------------
// AUTORITÉS MINT ET FREEZE
// --------------------------------------------------

function readUInt32LE(buffer, offset) {
  return buffer.readUInt32LE(offset);
}

function parseMintAccount(account) {
  if (!account || !account.data || !account.data[0]) {
    return {
      checked: false,
      error: "Compte mint introuvable"
    };
  }

  try {
    const data = Buffer.from(account.data[0], "base64");

    if (data.length < 82) {
      return {
        checked: false,
        error: "Données mint invalides"
      };
    }

    const mintAuthorityOption = readUInt32LE(data, 0);
    const supply = data.readBigUInt64LE(36).toString();
    const decimals = data.readUInt8(44);
    const initialized = data.readUInt8(45) === 1;
    const freezeAuthorityOption = readUInt32LE(data, 46);

    return {
      checked: true,
      initialized,
      mintAuthorityActive: mintAuthorityOption !== 0,
      freezeAuthorityActive: freezeAuthorityOption !== 0,
      supply,
      decimals
    };
  } catch (error) {
    return {
      checked: false,
      error: "Lecture mint impossible"
    };
  }
}

async function checkMintAuthorities(addresses) {
  const results = new Map();

  if (!HELIUS_RPC_URL || addresses.length === 0) {
    for (const address of addresses) {
      results.set(address, {
        checked: false,
        error: "Helius indisponible"
      });
    }
    return results;
  }

  try {
    // Un seul appel RPC groupé pour les tokens du scan.
    const response = await heliusRequest("getMultipleAccounts", [
      addresses,
      { encoding: "base64", commitment: "confirmed" }
    ]);

    const accounts = response && response.value
      ? response.value
      : [];

    addresses.forEach((address, index) => {
      results.set(
        address,
        parseMintAccount(accounts[index])
      );
    });
  } catch (error) {
    heliusError = error.message;

    for (const address of addresses) {
      results.set(address, {
        checked: false,
        error: error.message
      });
    }
  }

  return results;
}

// --------------------------------------------------
// DEX SCREENER
// --------------------------------------------------

async function discoverTokens() {
  const data = await fetchJson(CONFIG.DEX_PROFILES);

  if (!Array.isArray(data)) return [];

  const addresses = [];

  for (const item of data) {
    if (
      item &&
      item.chainId === "solana" &&
      item.tokenAddress &&
      !addresses.includes(item.tokenAddress)
    ) {
      addresses.push(item.tokenAddress);
    }

    if (addresses.length >= CONFIG.MAX_POOLS) break;
  }

  return addresses;
}

async function fetchTokenPairs(addresses) {
  const allPairs = [];

  // DEX Screener accepte des groupes d'adresses.
  for (let i = 0; i < addresses.length; i += 30) {
    const group = addresses.slice(i, i + 30);

    const url =
      CONFIG.DEX_TOKENS + group.join(",");

    const data = await fetchJson(url);

    if (Array.isArray(data)) {
      allPairs.push(...data);
    }

    await sleep(300);
  }

  return allPairs;
}

function chooseBestPair(address, pairs) {
  const matching = pairs.filter(pair => {
    if (!pair || pair.chainId !== "solana") return false;

    const base = pair.baseToken && pair.baseToken.address;
    const quote = pair.quoteToken && pair.quoteToken.address;

    return base === address || quote === address;
  });

  if (!matching.length) return null;

  matching.sort((a, b) =>
    number(b.liquidity && b.liquidity.usd) -
    number(a.liquidity && a.liquidity.usd)
  );

  return matching[0];
}

// --------------------------------------------------
// ANALYSE ET RISQUES
// --------------------------------------------------

function calculatePool(address, pair, mint) {
  const liquidity = number(pair.liquidity && pair.liquidity.usd);
  const volume = pair.volume || {};
  const txns = pair.txns || {};
  const change = pair.priceChange || {};

  const buys5m = number(txns.m5 && txns.m5.buys);
  const sells5m = number(txns.m5 && txns.m5.sells);

  const buys1h = number(txns.h1 && txns.h1.buys);
  const sells1h = number(txns.h1 && txns.h1.sells);

  const volume5m = number(volume.m5);
  const volume1h = number(volume.h1);

  const total1h = buys1h + sells1h;
  const buyRatio = total1h > 0
    ? buys1h / total1h
    : 0;

  const createdAt = number(pair.pairCreatedAt);
  const ageMinutes = createdAt > 0
    ? Math.max(0, (Date.now() - createdAt) / 60000)
    : null;

  const previous = snapshots.get(address);
  const acceleration = previous && previous.volume5m > 0
    ? volume5m / previous.volume5m
    : 1;

  snapshots.set(address, {
    volume5m,
    time: Date.now()
  });

  const risks = [];

  if (liquidity < CONFIG.MIN_LIQUIDITY_USD) {
    risks.push("LIQUIDITÉ FAIBLE");
  }

  if (volume1h < CONFIG.MIN_VOLUME_1H_USD) {
    risks.push("VOLUME FAIBLE");
  }

  if (ageMinutes !== null && ageMinutes < CONFIG.MIN_AGE_MINUTES) {
    risks.push("TOKEN TRÈS RÉCENT");
  }

  if (mint && mint.checked) {
    if (!mint.initialized) {
      risks.push("MINT NON INITIALISÉ");
    }

    if (mint.mintAuthorityActive) {
      risks.push("AUTORITÉ MINT ACTIVE");
    }

    if (mint.freezeAuthorityActive) {
      risks.push("AUTORITÉ FREEZE ACTIVE");
    }
  } else {
    risks.push("AUTORITÉS NON VÉRIFIÉES");
  }

  if (buys1h + sells1h < 5) {
    risks.push("ACTIVITÉ FAIBLE");
  }

  if (liquidity > 0 && volume1h > liquidity * 10) {
    risks.push("VOLUME ANORMAL");
  }

  const price = number(pair.priceUsd);

  // Estimation indicative d'impact de prix.
  // Ce n'est pas une cotation de swap.
  const estimatedImpact = liquidity > 0
    ? (CONFIG.TEST_TRADE_USD / (2 * liquidity)) * 100
    : null;

  let score = 0;

  score += Math.min(25, liquidity / 1000);
  score += Math.min(25, volume1h / 1000);
  score += Math.min(15, (buys1h + sells1h) / 4);
  score += Math.min(15, buyRatio * 15);

  const momentum = number(change.m5);
  score += Math.max(0, Math.min(10, momentum));

  score += Math.min(10, Math.max(0, acceleration - 1) * 5);

  score = Math.round(Math.min(100, score));

  const authorityVerified = Boolean(
    mint &&
    mint.checked &&
    mint.initialized &&
    !mint.mintAuthorityActive &&
    !mint.freezeAuthorityActive
  );

  const basicConditions =
    liquidity >= CONFIG.MIN_LIQUIDITY_USD &&
    volume1h >= CONFIG.MIN_VOLUME_1H_USD &&
    (ageMinutes === null || ageMinutes >= CONFIG.MIN_AGE_MINUTES);

  const buyEligible =
    authorityVerified &&
    basicConditions &&
    risks.length === 0;

  const token = pair.baseToken &&
    pair.baseToken.address === address
      ? pair.baseToken
      : pair.quoteToken;

  return {
    address,
    shortAddress: shorten(address),

    name: token && token.name ? token.name : "Token Solana",
    symbol: token && token.symbol ? token.symbol : "UNKNOWN",

    dex: pair.dexId || "unknown",
    pairAddress: pair.pairAddress || null,
    url: pair.url || null,

    priceUsd: price,
    liquidityUsd: round(liquidity),
    volume5mUsd: round(volume5m),
    volume1hUsd: round(volume1h),

    buys5m,
    sells5m,
    buys1h,
    sells1h,

    buyRatio: round(buyRatio * 100),
    change5m: round(momentum),
    change1h: round(number(change.h1)),

    ageMinutes: ageMinutes === null
      ? null
      : round(ageMinutes),

    acceleration: round(acceleration),

    score,

    mintSecurity: mint || {
      checked: false,
      error: "Non vérifié"
    },

    risks,

    authorityVerified,

    buyEligible,

    sellability: "NON VÉRIFIÉE",

    simulation: {
      tradeAmountUsd: CONFIG.TEST_TRADE_USD,
      estimatedImpactPercent: estimatedImpact === null
        ? null
        : round(estimatedImpact, 4),
      networkBaseFeeSol: 0.000005,
      priorityFee: "NON INCLUSE",
      swapFee: "NON INCLUSE",
      realQuote: false
    }
  };
}

// --------------------------------------------------
// SCAN PRINCIPAL
// --------------------------------------------------

async function scan() {
  if (!running || scanning) return;

  if (Date.now() < cooldownUntil) {
    scannerState = "COOLDOWN";
    return;
  }

  scanning = true;
  scannerState = "SCANNING";

  try {
    const addresses = await discoverTokens();

    if (!addresses.length) {
      pools = [];
      scannerState = "OK";
      lastUpdate = new Date().toISOString();
      lastError = null;
      return;
    }

    const pairs = await fetchTokenPairs(addresses);

    const mintResults = await checkMintAuthorities(addresses);

    const results = [];

    for (const address of addresses) {
      const pair = chooseBestPair(address, pairs);

      if (!pair) continue;

      const mint = mintResults.get(address);

      results.push(
        calculatePool(address, pair, mint)
      );
    }

    results.sort((a, b) => {
      if (a.buyEligible !== b.buyEligible) {
        return a.buyEligible ? -1 : 1;
      }
      return b.score - a.score;
    });

    pools = results;
    lastUpdate = new Date().toISOString();
    lastError = null;
    scannerState = "OK";

    // Nettoyage des anciennes observations.
    for (const [key, value] of snapshots.entries()) {
      if (Date.now() - value.time > 3600000) {
        snapshots.delete(key);
      }
    }

  } catch (error) {
    lastError = error.message;

    if (error.status === 429) {
      cooldownUntil = Date.now() + CONFIG.COOLDOWN_MS;
      scannerState = "COOLDOWN";
    } else {
      scannerState = "ERROR";
    }
  } finally {
    scanning = false;
  }
}

// --------------------------------------------------
// AUTOMATISATION
// --------------------------------------------------

function startScanner() {
  if (scanTimer) clearInterval(scanTimer);
  if (countdownTimer) clearInterval(countdownTimer);

  running = true;

  scan();

  scanTimer = setInterval(() => {
    scan();
  }, CONFIG.SCAN_INTERVAL);

  countdownTimer = setInterval(() => {
    if (cooldownUntil > Date.now()) {
      nextScanInSeconds = Math.ceil(
        (cooldownUntil - Date.now()) / 1000
      );
    } else {
      nextScanInSeconds = Math.max(
        0,
        Math.ceil(
          (CONFIG.SCAN_INTERVAL -
            ((Date.now() - new Date(lastUpdate || Date.now()).getTime()) %
              CONFIG.SCAN_INTERVAL)) / 1000
        )
      );
    }
  }, 1000);
}

function stopScanner() {
  running = false;

  if (scanTimer) clearInterval(scanTimer);
  if (countdownTimer) clearInterval(countdownTimer);

  scanTimer = null;
  countdownTimer = null;

  scannerState = "STOPPED";
}

// --------------------------------------------------
// ROUTES
// --------------------------------------------------

app.get("/", (req, res) => {
  res.json({
    name: "CART3R.JAILBREAK",
    status: "online",
    mode: "TEST",
    realTrading: false
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    running,
    scanner: scannerState
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
    nextScanInSeconds,

    helius: {
      configured: Boolean(HELIUS_RPC_URL),
      lastCheck: heliusLastCheck,
      lastSlot: heliusLastSlot,
      lastError: heliusError
    },

    protections: {
      mintAuthorityCheck: true,
      freezeAuthorityCheck: true,
      liquidityCheck: true,
      volumeCheck: true,
      suspiciousActivityCheck: true,
      sellabilitySimulation: false,
      realTrading: false
    }
  });
});

app.get("/pools", (req, res) => {
  res.json({
    ok: true,
    count: pools.length,
    mode: "TEST",
    pools
  });
});

app.get("/security", (req, res) => {
  res.json({
    mode: "TEST",
    realTrading: false,
    checkedPools: pools.length,
    eligiblePools: pools.filter(p => p.buyEligible).length,
    blockedPools: pools.filter(p => !p.buyEligible).length,
    pools: pools.map(p => ({
      name: p.name,
      symbol: p.symbol,
      address: p.address,
      score: p.score,
      buyEligible: p.buyEligible,
      authorityVerified: p.authorityVerified,
      mintSecurity: p.mintSecurity,
      risks: p.risks,
      sellability: p.sellability,
      simulation: p.simulation
    }))
  });
});

app.get("/helius-test", async (req, res) => {
  const ok = await checkHelius();

  res.status(ok ? 200 : 503).json({
    ok,
    slot: heliusLastSlot,
    checkedAt: heliusLastCheck,
    error: heliusError
  });
});

app.post("/test/start", (req, res) => {
  startScanner();

  res.json({
    ok: true,
    running,
    mode: "TEST",
    realTrading: false
  });
});

app.post("/test/stop", (req, res) => {
  stopScanner();

  res.json({
    ok: true,
    running,
    mode: "TEST",
    realTrading: false
  });
});

// --------------------------------------------------
// DÉMARRAGE
// --------------------------------------------------

app.listen(PORT, () => {
  console.log("CART3R.JAILBREAK démarré sur le port " + PORT);
  console.log("MODE TEST - TRADING RÉEL DÉSACTIVÉ");

  startScanner();
});
