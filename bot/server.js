
"use strict";

const express = require("express");
const app = express();

app.use(express.json({ limit: "100kb" }));

const PORT = process.env.PORT || 3000;

const HELIUS_RPC_URL = process.env.HELIUS_RPC_URL || "";
const JUPITER_API_KEY = process.env.JUPITER_API_KEY || "";

const SOL_MINT = "So11111111111111111111111111111111111111112";
const USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

const TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

const CONFIG = {
  MAX_POOLS: 100,

  // Jupiter : limitation volontaire des appels
  MAX_JUPITER_CHECKS: 3,
  JUPITER_REQUEST_GAP_MS: 1500,
  JUPITER_COOLDOWN_MS: 60000,
  SOL_PRICE_CACHE_MS: 300000,

  SCAN_INTERVAL: 300000,
  ADDRESSES_CACHE_MS: 1800000,
  PAIRS_CACHE_MS: 300000,
  DEX_COOLDOWN_MS: 300000,
  REQUEST_TIMEOUT: 15000,

  MIN_LIQUIDITY_USD: 5000,
  MIN_VOLUME_1H_USD: 1000,
  MIN_AGE_MINUTES: 5,
  MIN_TRANSACTIONS_1H: 5,
  MIN_SCORE: 50,
  MIN_BUY_RATIO: 50,

  TEST_TRADE_USD: 10,
  MAX_JUPITER_IMPACT_PCT: 5,

  MAX_VOLUME_LIQUIDITY_RATIO: 10,

  PROFILES_URL:
    "https://api.dexscreener.com/token-profiles/latest/v1",

  TOKENS_URL:
    "https://api.dexscreener.com/tokens/v1/solana/"
};

// --------------------------------------------------
// VARIABLES
// --------------------------------------------------

let running = true;
let scanning = false;
let scannerState = "STARTING";

let pools = [];
let lastUpdate = null;
let lastError = null;

let dexCooldownUntil = 0;
let jupiterCooldownUntil = 0;

let nextScanAt = Date.now();

let heliusLastSlot = null;
let heliusLastCheck = null;
let heliusError = null;

let jupiterLastCheck = null;
let jupiterLastError = null;
let jupiterLastSolPriceUsd = null;

let jupiterChecksCount = 0;
let jupiterSuccessCount = 0;

let jupiterRequestAt = 0;
let jupiterQueue = Promise.resolve();

let solPriceCached = null;
let solPriceFetchedAt = 0;

let scanTimer = null;
let heliusTimer = null;
let countdownTimer = null;

let nextScanInSeconds = 0;

let cachedAddresses = [];
let addressesFetchedAt = 0;

let cachedPairs = [];
let pairsFetchedAt = 0;
let pairsCacheKey = "";

const snapshots = new Map();

// --------------------------------------------------
// OUTILS
// --------------------------------------------------

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function num(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function round(value, decimals = 2) {
  const p = Math.pow(10, decimals);
  return Math.round(num(value) * p) / p;
}

function shorten(value) {
  if (!value || typeof value !== "string") return "";
  return value.slice(0, 6) + "..." + value.slice(-4);
}

function isDexCooldown() {
  return Date.now() < dexCooldownUntil;
}

function isJupiterCooldown() {
  return Date.now() < jupiterCooldownUntil;
}

function setDexCooldown() {
  dexCooldownUntil = Math.max(
    dexCooldownUntil,
    Date.now() + CONFIG.DEX_COOLDOWN_MS
  );
}

function setJupiterCooldown(ms = CONFIG.JUPITER_COOLDOWN_MS) {
  jupiterCooldownUntil = Math.max(
    jupiterCooldownUntil,
    Date.now() + ms
  );
}

function retryAfterMs(response) {
  const value = response.headers.get("retry-after");

  if (!value) return CONFIG.JUPITER_COOLDOWN_MS;

  const seconds = Number(value);

  if (Number.isFinite(seconds) && seconds > 0) {
    return Math.max(1000, seconds * 1000);
  }

  const date = Date.parse(value);

  if (Number.isFinite(date)) {
    return Math.max(1000, date - Date.now());
  }

  return CONFIG.JUPITER_COOLDOWN_MS;
}

// --------------------------------------------------
// DEX SCREENER
// --------------------------------------------------

async function fetchJson(url) {
  const controller = new AbortController();

  const timeout = setTimeout(
    () => controller.abort(),
    CONFIG.REQUEST_TIMEOUT
  );

  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        Accept: "application/json"
      }
    });

    if (response.status === 429) {
      setDexCooldown();

      const error = new Error(
        "Limite DEX Screener : pause de 300 secondes"
      );

      error.status = 429;
      throw error;
    }

    if (!response.ok) {
      throw new Error(
        "DEX Screener HTTP " + response.status
      );
    }

    return await response.json();

  } finally {
    clearTimeout(timeout);
  }
}

// --------------------------------------------------
// HELIUS
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
      headers: {
        "Content-Type": "application/json"
      },
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
      throw new Error(
        data.error.message || "Erreur Helius"
      );
    }

    return data.result;

  } finally {
    clearTimeout(timeout);
  }
}

async function checkHelius() {
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
// SECURITE DES MINTS
// --------------------------------------------------

async function checkMintAuthorities(addresses) {
  const results = new Map();

  if (!HELIUS_RPC_URL || !addresses.length) {
    for (const address of addresses) {
      results.set(address, {
        checked: false,
        error: "Helius indisponible"
      });
    }

    return results;
  }

  try {
    // getMultipleAccounts accepte jusqu'à 100 comptes.
    const response = await heliusRequest(
      "getMultipleAccounts",
      [
        addresses.slice(0, 100),
        {
          encoding: "base64",
          commitment: "confirmed"
        }
      ]
    );

    const accounts = response && response.value
      ? response.value
      : [];

    addresses.forEach((address, index) => {
      const account = accounts[index];

      if (!account || !account.data || !account.data[0]) {
        results.set(address, {
          checked: false,
          error: "Compte mint introuvable"
        });
        return;
      }

      try {
        const owner = account.owner;

        if (
          owner !== TOKEN_PROGRAM &&
          owner !== TOKEN_2022_PROGRAM
        ) {
          throw new Error("Programme Token non reconnu");
        }

        const data = Buffer.from(
          account.data[0],
          "base64"
        );

        if (data.length < 82) {
          throw new Error("Données mint invalides");
        }

        const mintAuthorityOption =
          data.readUInt32LE(0);

        const freezeAuthorityOption =
          data.readUInt32LE(46);

        const initialized = data.readUInt8(45) === 1;

        results.set(address, {
          checked: true,
          owner,

          token2022: owner === TOKEN_2022_PROGRAM,

          // Les extensions Token-2022 ne sont pas
          // intégralement analysées par ce contrôle.
          extensionsChecked: false,

          initialized,

          mintAuthorityActive:
            mintAuthorityOption !== 0,

          freezeAuthorityActive:
            freezeAuthorityOption !== 0,

          decimals: data.readUInt8(44),

          supply: data.readBigUInt64LE(36).toString()
        });

      } catch (error) {
        results.set(address, {
          checked: false,
          error: error.message
        });
      }
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
// FILE D'ATTENTE JUPITER
// --------------------------------------------------

async function waitForJupiterSlot() {
  const task = jupiterQueue.then(async () => {
    if (isJupiterCooldown()) {
      const error = new Error("Jupiter en pause après limitation API");
      error.status = 429;
      throw error;
    }

    const wait = Math.max(
      0,
      CONFIG.JUPITER_REQUEST_GAP_MS -
        (Date.now() - jupiterRequestAt)
    );

    if (wait > 0) {
      await sleep(wait);
    }

    if (isJupiterCooldown()) {
      const error = new Error("Jupiter en pause après limitation API");
      error.status = 429;
      throw error;
    }

    jupiterRequestAt = Date.now();
  });

  jupiterQueue = task.catch(() => {});

  await task;
}

// --------------------------------------------------
// JUPITER SWAP V2
// --------------------------------------------------

async function jupiterOrder(inputMint, outputMint, amount) {
  if (!JUPITER_API_KEY) {
    throw new Error("JUPITER_API_KEY non configurée");
  }

  await waitForJupiterSlot();

  const params = new URLSearchParams({
    inputMint,
    outputMint,
    amount: String(amount)
  });

  const controller = new AbortController();

  const timeout = setTimeout(
    () => controller.abort(),
    CONFIG.REQUEST_TIMEOUT
  );

  try {
    const response = await fetch(
      "https://api.jup.ag/swap/v2/order?" + params,
      {
        method: "GET",
        headers: {
          Accept: "application/json",
          "x-api-key": JUPITER_API_KEY
        },
        signal: controller.signal
      }
    );

    const body = await response.text();

    let data;

    try {
      data = JSON.parse(body);
    } catch {
      throw new Error("Réponse Jupiter invalide");
    }

    if (response.status === 429) {
      const pause = retryAfterMs(response);

      setJupiterCooldown(pause);

      const error = new Error("Jupiter HTTP 429");
      error.status = 429;
      error.retryAfterMs = pause;

      jupiterLastError = error.message;

      throw error;
    }

    if (!response.ok) {
      const error = new Error(
        data.errorMessage ||
        data.error ||
        "Jupiter HTTP " + response.status
      );

      error.status = response.status;
      throw error;
    }

    if (data.errorCode || data.errorMessage) {
      throw new Error(
        data.errorMessage || "Erreur Jupiter"
      );
    }

    if (
      !data.outAmount ||
      BigInt(data.outAmount) <= 0n
    ) {
      throw new Error("Aucune quantité de sortie Jupiter");
    }

    jupiterLastCheck = new Date().toISOString();

    return data;

  } finally {
    clearTimeout(timeout);
  }
}

// --------------------------------------------------
// PRIX SOL AVEC CACHE
// --------------------------------------------------

async function getJupiterSolPrice() {
  const cacheValid =
    solPriceCached !== null &&
    Date.now() - solPriceFetchedAt <
      CONFIG.SOL_PRICE_CACHE_MS;

  if (cacheValid) {
    return solPriceCached;
  }

  const quote = await jupiterOrder(
    SOL_MINT,
    USDC_MINT,
    "1000000000"
  );

  const price = Number(quote.outAmount) / 1000000;

  if (!Number.isFinite(price) || price <= 0) {
    throw new Error("Prix SOL invalide");
  }

  solPriceCached = price;
  solPriceFetchedAt = Date.now();

  jupiterLastSolPriceUsd = round(price, 4);
  jupiterLastError = null;

  return price;
}

// --------------------------------------------------
// IMPACT JUPITER
// --------------------------------------------------

function getPriceImpactPercent(quote) {
  const raw =
    quote.priceImpactPct !== undefined
      ? quote.priceImpactPct
      : quote.priceImpact;

  if (raw === undefined || raw === null) {
    return null;
  }

  const value = Number(raw);

  if (!Number.isFinite(value)) {
    return null;
  }

  return value <= 1
    ? Math.abs(value * 100)
    : Math.abs(value);
}

// --------------------------------------------------
// TEST ACHAT + REVENTE
// --------------------------------------------------

async function checkJupiterSellability(pool, solPrice) {
  const checkedAt = new Date().toISOString();

  jupiterChecksCount++;

  try {
    const solAmount = Math.floor(
      CONFIG.TEST_TRADE_USD / solPrice * 1e9
    );

    if (solAmount <= 0) {
      throw new Error("Montant SOL invalide");
    }

    // ACHAT THEORIQUE

    const buy = await jupiterOrder(
      SOL_MINT,
      pool.address,
      String(solAmount)
    );

    const tokenAmount = BigInt(buy.outAmount);

    if (tokenAmount <= 0n) {
      throw new Error("Quantité achetable invalide");
    }

    // REVENTE THEORIQUE

    const sell = await jupiterOrder(
      pool.address,
      SOL_MINT,
      tokenAmount.toString()
    );

    const sellSol = Number(sell.outAmount) / 1e9;
    const initialSol = solAmount / 1e9;

    if (
      !Number.isFinite(sellSol) ||
      sellSol <= 0
    ) {
      throw new Error("Montant de revente invalide");
    }

    const returnPercent =
      ((sellSol - initialSol) / initialSol) * 100;

    const buyImpact = getPriceImpactPercent(buy);
    const sellImpact = getPriceImpactPercent(sell);

    const impacts = [
      buyImpact,
      sellImpact
    ].filter(value => value !== null);

    const maxImpact = impacts.length
      ? Math.max(...impacts)
      : null;

    if (
      maxImpact !== null &&
      maxImpact > CONFIG.MAX_JUPITER_IMPACT_PCT
    ) {
      throw new Error(
        "Impact excessif : " +
        round(maxImpact, 3) + "%"
      );
    }

    if (maxImpact === null) {
      throw new Error(
        "Impact Jupiter non disponible"
      );
    }

    // Estimation de deux frais réseau de base.
    // Les frais prioritaires ne sont pas inclus.

    const estimatedNetworkFeesSol = 0.00001;

    const netSellSol = Math.max(
      0,
      sellSol - estimatedNetworkFeesSol
    );

    const netReturnPercent =
      ((netSellSol - initialSol) / initialSol) * 100;

    pool.sellability =
      "ACHAT + VENTE COTÉS (SIMULATION)";

    pool.jupiter = {
      verified: true,
      checkedAt,

      routerBuy: buy.router || null,
      routerSell: sell.router || null,

      buyAmount: buy.inAmount,
      tokenAmount: buy.outAmount,
      sellOutputSol: sell.outAmount,

      buyPriceImpactPercent: round(buyImpact, 4),
      sellPriceImpactPercent: round(sellImpact, 4),

      maxPriceImpactPercent: round(maxImpact, 4),

      roundTripReturnPercent:
        round(returnPercent, 3),

      estimatedNetReturnPercent:
        round(netReturnPercent, 3),

      quoteOnly: true,
      executionPerformed: false
    };

    pool.simulation = {
      tradeAmountUsd: CONFIG.TEST_TRADE_USD,
      tradeAmountSol: round(initialSol, 9),

      estimatedSellValueUsd:
        round(sellSol * solPrice),

      estimatedSellValueAfterBaseFeesUsd:
        round(netSellSol * solPrice),

      estimatedNetworkFeesSol,

      roundTripReturnPercent:
        round(returnPercent, 3),

      estimatedNetReturnPercent:
        round(netReturnPercent, 3),

      priorityFee: "NON INCLUSE",
      executionPerformed: false,
      realQuote: true,
      quoteOnly: true
    };

    pool.buyEligible = true;

    jupiterSuccessCount++;
    jupiterLastError = null;

    return true;

  } catch (error) {
    pool.sellability = "VENTE NON VALIDÉE";
    pool.buyEligible = false;

    pool.jupiter = {
      verified: false,
      checkedAt,
      error: error.message,
      executionPerformed: false
    };

    pool.simulation = {
      ...pool.simulation,
      realQuote: false,
      quoteOnly: true,
      executionPerformed: false
    };

    jupiterLastError = error.message;

    return false;
  }
}

// --------------------------------------------------
// ADRESSES
// --------------------------------------------------

async function getAddresses() {
  const cacheValid =
    cachedAddresses.length > 0 &&
    Date.now() - addressesFetchedAt <
      CONFIG.ADDRESSES_CACHE_MS;

  if (cacheValid) {
    return cachedAddresses;
  }

  try {
    const data = await fetchJson(CONFIG.PROFILES_URL);

    if (!Array.isArray(data)) {
      throw new Error("Réponse DEX invalide");
    }

    const unique = new Set();

    for (const item of data) {
      if (
        item &&
        item.chainId === "solana" &&
        item.tokenAddress
      ) {
        unique.add(item.tokenAddress);
      }

      if (unique.size >= CONFIG.MAX_POOLS) {
        break;
      }
    }

    const addresses = Array.from(unique);

    if (!addresses.length) {
      throw new Error("Aucun token Solana trouvé");
    }

    cachedAddresses = addresses;
    addressesFetchedAt = Date.now();

    return cachedAddresses;

  } catch (error) {
    if (cachedAddresses.length > 0) {
      console.log("Utilisation du cache des adresses");
      return cachedAddresses;
    }

    throw error;
  }
}

// --------------------------------------------------
// PAIRES DEX
// --------------------------------------------------

async function getPairs(addresses) {
  const key = addresses.slice().sort().join(",");

  const cacheValid =
    cachedPairs.length > 0 &&
    pairsCacheKey === key &&
    Date.now() - pairsFetchedAt <
      CONFIG.PAIRS_CACHE_MS;

  if (cacheValid) {
    return cachedPairs;
  }

  try {
    const allPairs = [];

    // Maximum 30 adresses par requête.

    for (let i = 0; i < addresses.length; i += 30) {
      const group = addresses.slice(i, i + 30);

      const url =
        CONFIG.TOKENS_URL + group.join(",");

      const data = await fetchJson(url);

      if (Array.isArray(data)) {
        allPairs.push(...data);
      }
    }

    if (!allPairs.length) {
      throw new Error("Aucune paire reçue");
    }

    cachedPairs = allPairs;
    pairsFetchedAt = Date.now();
    pairsCacheKey = key;

    return cachedPairs;

  } catch (error) {
    if (
      cachedPairs.length > 0 &&
      pairsCacheKey === key
    ) {
      console.log("Utilisation du cache des paires");
      return cachedPairs;
    }

    throw error;
  }
}

// --------------------------------------------------
// MEILLEURE PAIRE
// --------------------------------------------------

function chooseBestPair(address, pairs) {
  const matching = pairs.filter(pair => {
    if (!pair || pair.chainId !== "solana") {
      return false;
    }

    const base =
      pair.baseToken && pair.baseToken.address;

    const quote =
      pair.quoteToken && pair.quoteToken.address;

    return base === address || quote === address;
  });

  if (!matching.length) return null;

  matching.sort((a, b) => {
    const liquidityDifference =
      num(b.liquidity && b.liquidity.usd) -
      num(a.liquidity && a.liquidity.usd);

    return liquidityDifference;
  });

  return matching[0];
}

// --------------------------------------------------
// ANALYSE DES POOLS
// --------------------------------------------------

function analyzePool(address, pair, mint) {
  const liquidity = num(
    pair.liquidity && pair.liquidity.usd
  );

  const volume = pair.volume || {};
  const txns = pair.txns || {};
  const changes = pair.priceChange || {};

  const buys5m = num(txns.m5 && txns.m5.buys);
  const sells5m = num(txns.m5 && txns.m5.sells);

  const buys1h = num(txns.h1 && txns.h1.buys);
  const sells1h = num(txns.h1 && txns.h1.sells);

  const volume5m = num(volume.m5);
  const volume1h = num(volume.h1);

  const totalTransactions = buys1h + sells1h;

  const buyRatio = totalTransactions > 0
    ? buys1h / totalTransactions
    : 0;

  const sellRatio = totalTransactions > 0
    ? sells1h / totalTransactions
    : 0;

  const createdAt = num(pair.pairCreatedAt);

  const ageMinutes = createdAt > 0
    ? Math.max(
        0,
        (Date.now() - createdAt) / 60000
      )
    : null;

  const previous = snapshots.get(address);

  const acceleration =
    previous && previous.volume5m > 0
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

  if (ageMinutes === null) {
    risks.push("ÂGE INCONNU");
  } else if (ageMinutes < CONFIG.MIN_AGE_MINUTES) {
    risks.push("TOKEN TRÈS RÉCENT");
  }

  if (!mint || !mint.checked) {
    risks.push("AUTORITÉS NON VÉRIFIÉES");
  } else {
    if (!mint.initialized) {
      risks.push("MINT NON INITIALISÉ");
    }

    if (mint.mintAuthorityActive) {
      risks.push("AUTORITÉ MINT ACTIVE");
    }

    if (mint.freezeAuthorityActive) {
      risks.push("AUTORITÉ FREEZE ACTIVE");
    }
  }

  if (totalTransactions < CONFIG.MIN_TRANSACTIONS_1H) {
    risks.push("ACTIVITÉ FAIBLE");
  }

  if (
    totalTransactions >= 10 &&
    buyRatio * 100 < CONFIG.MIN_BUY_RATIO
  ) {
    risks.push("PRESSION ACHETEUSE FAIBLE");
  }

  if (
    totalTransactions >= 10 &&
    sells1h > buys1h * 2
  ) {
    risks.push("PRESSION VENDEUSE");
  }

  if (
    liquidity > 0 &&
    volume1h > liquidity * CONFIG.MAX_VOLUME_LIQUIDITY_RATIO
  ) {
    risks.push("VOLUME ANORMAL");
  }

  const momentum = num(changes.m5);
  const momentum1h = num(changes.h1);

  // SCORE SUR 100

  let score = 0;

  // Liquidité : 20 points

  score += Math.min(20, liquidity / 2500);

  // Volume : 20 points

  score += Math.min(20, volume1h / 2000);

  // Activité : 15 points

  score += Math.min(15, totalTransactions / 4);

  // Pression acheteuse : 20 points

  score += buyRatio * 20;

  // Momentum : 10 points

  score += Math.max(0, Math.min(10, momentum));

  // Accélération du volume : 10 points

  score += Math.min(
    10,
    Math.max(0, acceleration - 1) * 5
  );

  // Ancienneté : 5 points

  if (
    ageMinutes !== null &&
    ageMinutes >= 10 &&
    ageMinutes <= 1440
  ) {
    score += 5;
  }

  // Pénalités

  if (momentum < -5) score -= 5;
  if (sellRatio > 0.65) score -= 8;
  if (momentum1h < -15) score -= 5;

  score = Math.round(
    Math.max(0, Math.min(100, score))
  );

  if (score < CONFIG.MIN_SCORE) {
    risks.push("SCORE INSUFFISANT");
  }

  const authorityVerified = Boolean(
    mint &&
    mint.checked &&
    mint.initialized &&
    !mint.mintAuthorityActive &&
    !mint.freezeAuthorityActive
  );

  const buyEligible =
    authorityVerified &&
    risks.length === 0;

  const token =
    pair.baseToken &&
    pair.baseToken.address === address
      ? pair.baseToken
      : pair.quoteToken;

  const estimatedImpact =
    liquidity > 0
      ? CONFIG.TEST_TRADE_USD / (2 * liquidity) * 100
      : null;

  return {
    address,
    shortAddress: shorten(address),

    name: token && token.name
      ? token.name
      : "Token Solana",

    symbol: token && token.symbol
      ? token.symbol
      : "UNKNOWN",

    dex: pair.dexId || "unknown",
    pairAddress: pair.pairAddress || null,
    url: pair.url || null,

    priceUsd: num(pair.priceUsd),

    liquidityUsd: round(liquidity),
    volume5mUsd: round(volume5m),
    volume1hUsd: round(volume1h),

    buys5m,
    sells5m,
    buys1h,
    sells1h,

    buyRatio: round(buyRatio * 100),
    sellRatio: round(sellRatio * 100),

    change5m: round(momentum),
    change1h: round(momentum1h),

    ageMinutes: ageMinutes === null
      ? null
      : round(ageMinutes),

    acceleration: round(acceleration, 3),

    score,

    mintSecurity: mint || {
      checked: false,
      error: "Non vérifié"
    },

    authorityVerified,
    risks,
    buyEligible,

    sellability: "NON VÉRIFIÉE",

    jupiter: {
      verified: false,
      error: null
    },

    simulation: {
      tradeAmountUsd: CONFIG.TEST_TRADE_USD,

      estimatedImpactPercent:
        estimatedImpact === null
          ? null
          : round(estimatedImpact, 4),

      networkBaseFeeSol: 0.000005,

      priorityFee: "NON INCLUSE",
      swapFee: "VOIR COTATION JUPITER",

      realQuote: false,
      quoteOnly: true,
      executionPerformed: false
    }
  };
}

// --------------------------------------------------
// SCANNER PRINCIPAL
// --------------------------------------------------

async function scan() {
  if (!running || scanning) return;

  if (isDexCooldown()) {
    scannerState = "COOLDOWN";
    scheduleNextScan(
      Math.max(1000, dexCooldownUntil - Date.now())
    );
    return;
  }

  scanning = true;
  scannerState = "SCANNING";

  try {
    const addresses = await getAddresses();

    const pairs = await getPairs(addresses);

    const mintResults =
      await checkMintAuthorities(addresses);

    const results = [];

    for (const address of addresses) {
      const pair = chooseBestPair(address, pairs);

      if (!pair) continue;

      results.push(
        analyzePool(
          address,
          pair,
          mintResults.get(address)
        )
      );
    }

    // PREMIER CLASSEMENT

    results.sort((a, b) => b.score - a.score);

    jupiterChecksCount = 0;
    jupiterSuccessCount = 0;

    // On ne vérifie que les meilleurs candidats
    // qui passent déjà les contrôles fondamentaux.

    const candidates = results
      .filter(pool => pool.buyEligible)
      .slice(0, CONFIG.MAX_JUPITER_CHECKS);

    if (!JUPITER_API_KEY) {
      for (const pool of candidates) {
        pool.buyEligible = false;
        pool.sellability = "JUPITER NON CONFIGURÉ";

        pool.jupiter = {
          verified: false,
          error: "JUPITER_API_KEY non configurée"
        };
      }

      jupiterLastError = "Clé API non configurée";

    } else if (candidates.length > 0) {
      try {
        const solPrice = await getJupiterSolPrice();

        for (const pool of candidates) {
          if (isJupiterCooldown()) {
            pool.buyEligible = false;
            pool.sellability = "JUPITER EN PAUSE";

            pool.jupiter = {
              verified: false,
              error: "Jupiter en pause après limitation API"
            };

            continue;
          }

          const success =
            await checkJupiterSellability(
              pool,
              solPrice
            );

          // En cas de 429, on arrête immédiatement
          // les autres vérifications Jupiter du scan.

          if (
            !success &&
            pool.jupiter &&
            pool.jupiter.error === "Jupiter HTTP 429"
          ) {
            break;
          }
        }

      } catch (error) {
        jupiterLastError = error.message;

        for (const pool of candidates) {
          if (!pool.jupiter.verified) {
            pool.buyEligible = false;
            pool.sellability = "JUPITER NON DISPONIBLE";

            pool.jupiter = {
              verified: false,
              error: error.message
            };
          }
        }
      }
    }

    // CLASSEMENT FINAL

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

    // NETTOYAGE DES SNAPSHOTS

    for (const [key, value] of snapshots.entries()) {
      if (Date.now() - value.time > 3600000) {
        snapshots.delete(key);
      }
    }

  } catch (error) {
    lastError = error.message;

    if (error.status === 429) {
      setDexCooldown();
      scannerState = "COOLDOWN";
    } else {
      scannerState = "ERROR";
    }

  } finally {
    scanning = false;

    const cooldownRemaining = Math.max(
      dexCooldownUntil - Date.now(),
      0
    );

    scheduleNextScan(
      Math.max(
        CONFIG.SCAN_INTERVAL,
        cooldownRemaining
      )
    );
  }
}

// --------------------------------------------------
// PROGRAMMATION DU SCANNER
// --------------------------------------------------

function scheduleNextScan(delay) {
  if (!running) return;

  if (scanTimer) {
    clearTimeout(scanTimer);
  }

  const safeDelay = Math.max(1000, delay);

  nextScanAt = Date.now() + safeDelay;

  scanTimer = setTimeout(() => {
    scanTimer = null;
    scan();
  }, safeDelay);
}

// --------------------------------------------------
// DEMARRAGE / ARRET
// --------------------------------------------------

function startScanner() {
  if (scanTimer) clearTimeout(scanTimer);
  if (heliusTimer) clearInterval(heliusTimer);
  if (countdownTimer) clearInterval(countdownTimer);

  running = true;
  nextScanAt = Date.now();

  scan();
  checkHelius();

  heliusTimer = setInterval(() => {
    checkHelius();
  }, 300000);

  countdownTimer = setInterval(() => {
    const now = Date.now();

    if (dexCooldownUntil > now) {
      nextScanInSeconds = Math.ceil(
        (dexCooldownUntil - now) / 1000
      );
    } else {
      nextScanInSeconds = Math.max(
        0,
        Math.ceil((nextScanAt - now) / 1000)
      );
    }
  }, 1000);
}

function stopScanner() {
  running = false;

  if (scanTimer) clearTimeout(scanTimer);
  if (heliusTimer) clearInterval(heliusTimer);
  if (countdownTimer) clearInterval(countdownTimer);

  scanTimer = null;
  heliusTimer = null;
  countdownTimer = null;

  nextScanInSeconds = 0;
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

// --------------------------------------------------
// STATUS
// --------------------------------------------------

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

    jupiter: {
      configured: Boolean(JUPITER_API_KEY),
      api: "SWAP_V2",

      lastCheck: jupiterLastCheck,
      lastSolPriceUsd: jupiterLastSolPriceUsd,

      checksCount: jupiterChecksCount,
      successCount: jupiterSuccessCount,

      cooldownSeconds: Math.max(
        0,
        Math.ceil(
          (jupiterCooldownUntil - Date.now()) / 1000
        )
      ),

      lastError: jupiterLastError
    },

    cache: {
      addresses: cachedAddresses.length,
      pairs: cachedPairs.length,

      addressesAgeSeconds: addressesFetchedAt
        ? Math.floor(
            (Date.now() - addressesFetchedAt) / 1000
          )
        : null,

      pairsAgeSeconds: pairsFetchedAt
        ? Math.floor(
            (Date.now() - pairsFetchedAt) / 1000
          )
        : null
    },

    protections: {
      mintAuthorityCheck: true,
      freezeAuthorityCheck: true,
      tokenProgramCheck: true,

      token2022ExtensionsFullyChecked: false,

      liquidityCheck: true,
      volumeCheck: true,
      suspiciousActivityCheck: true,

      buyPressureCheck: true,
      scoreCheck: true,

      sellabilityQuoteCheck: true,
      sellabilitySimulation: false,

      realTrading: false
    }
  });
});

// --------------------------------------------------
// POOLS
// --------------------------------------------------

app.get("/pools", (req, res) => {
  res.json({
    ok: true,
    count: pools.length,
    maxPools: CONFIG.MAX_POOLS,
    mode: "TEST",
    realTrading: false,
    pools
  });
});

// --------------------------------------------------
// SECURITE
// --------------------------------------------------

app.get("/security", (req, res) => {
  res.json({
    mode: "TEST",
    realTrading: false,

    checkedPools: pools.length,

    eligiblePools: pools.filter(
      p => p.buyEligible
    ).length,

    blockedPools: pools.filter(
      p => !p.buyEligible
    ).length,

    jupiter: {
      configured: Boolean(JUPITER_API_KEY),

      checksCount: jupiterChecksCount,
      successCount: jupiterSuccessCount,

      lastCheck: jupiterLastCheck,
      lastError: jupiterLastError,

      cooldownSeconds: Math.max(
        0,
        Math.ceil(
          (jupiterCooldownUntil - Date.now()) / 1000
        )
      )
    },

    pools: pools.map(p => ({
      name: p.name,
      symbol: p.symbol,
      address: p.address,

      score: p.score,

      liquidityUsd: p.liquidityUsd,
      volume1hUsd: p.volume1hUsd,

      buyRatio: p.buyRatio,
      sellRatio: p.sellRatio,

      change5m: p.change5m,
      change1h: p.change1h,

      buyEligible: p.buyEligible,
      authorityVerified: p.authorityVerified,

      mintSecurity: p.mintSecurity,
      risks: p.risks,

      sellability: p.sellability,

      jupiter: p.jupiter,
      simulation: p.simulation
    }))
  });
});

// --------------------------------------------------
// TEST HELIUS
// --------------------------------------------------

app.get("/helius-test", async (req, res) => {
  const ok = await checkHelius();

  res.status(ok ? 200 : 503).json({
    ok,
    slot: heliusLastSlot,
    checkedAt: heliusLastCheck,
    error: heliusError
  });
});

// --------------------------------------------------
// TEST JUPITER
// --------------------------------------------------

app.get("/jupiter-test", async (req, res) => {
  try {
    if (!JUPITER_API_KEY) {
      return res.status(503).json({
        ok: false,
        configured: false,
        error: "JUPITER_API_KEY non configurée"
      });
    }

    const price = await getJupiterSolPrice();

    jupiterLastCheck =
      new Date().toISOString();

    jupiterLastError = null;

    res.json({
      ok: true,
      configured: true,
      api: "SWAP_V2",
      solPriceUsd: price,
      checkedAt: jupiterLastCheck,
      realTrading: false
    });

  } catch (error) {
    jupiterLastError = error.message;

    res.status(503).json({
      ok: false,
      configured: true,
      api: "SWAP_V2",
      error: error.message,
      realTrading: false
    });
  }
});

// --------------------------------------------------
// CONTROLES SCANNER
// --------------------------------------------------

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
// SERVEUR
// --------------------------------------------------

app.listen(PORT, () => {
  console.log("CART3R.JAILBREAK V3 démarré");
  console.log("MAXIMUM 100 POOLS");

  console.log(
    "HELIUS : " +
    (HELIUS_RPC_URL ? "CONFIGURÉ" : "ABSENT")
  );

  console.log(
    "JUPITER V2 : " +
    (JUPITER_API_KEY ? "CONFIGURÉ" : "ABSENT")
  );

  console.log("MODE TEST");
  console.log("TRADING REEL DESACTIVE");

  startScanner();
});
