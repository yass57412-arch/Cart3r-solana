
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

const GECKO_BASE = "https://api.geckoterminal.com/api/v2";

const CONFIG = {
  MAX_POOLS: 100,
  MAX_CATALOG_POOLS: 300,
  CATALOG_TTL_MS: 3600000,

  // GECKOTERMINAL
  GECKO_REQUEST_GAP_MS: 65000,
  GECKO_CACHE_MS: 240000,
  GECKO_STALE_MAX_MS: 3600000,
  GECKO_COOLDOWN_MS: 120000,
  GECKO_MAX_COOLDOWN_MS: 1800000,

  // JUPITER
  MAX_JUPITER_CHECKS: 3,
  JUPITER_REQUEST_GAP_MS: 2000,
  JUPITER_COOLDOWN_MS: 120000,
  SOL_PRICE_CACHE_MS: 300000,

  // SCANNER
  SCAN_INTERVAL: 300000,
  REQUEST_TIMEOUT: 20000,

  // FILTRES
  MIN_LIQUIDITY_USD: 5000,
  MIN_VOLUME_1H_USD: 1000,
  MIN_AGE_MINUTES: 5,
  MIN_TRANSACTIONS_1H: 5,
  MIN_SCORE: 45,
  MIN_BUY_RATIO: 50,
  MAX_VOLUME_LIQUIDITY_RATIO: 10,

  // SIMULATION
  TEST_TRADE_USD: 10,
  MAX_JUPITER_IMPACT_PCT: 5
};

// --------------------------------------------------
// FLUX GECKOTERMINAL
// Un seul flux interrogé par scan.
// --------------------------------------------------

const GECKO_FEEDS = [
  {
    name: "new_1",
    url: GECKO_BASE +
      "/networks/solana/new_pools?page=1&include=base_token,quote_token,dex"
  },
  {
    name: "new_2",
    url: GECKO_BASE +
      "/networks/solana/new_pools?page=2&include=base_token,quote_token,dex"
  },
  {
    name: "trending_1",
    url: GECKO_BASE +
      "/networks/solana/trending_pools?page=1&duration=1h&include=base_token,quote_token,dex"
  },
  {
    name: "trending_2",
    url: GECKO_BASE +
      "/networks/solana/trending_pools?page=2&duration=1h&include=base_token,quote_token,dex"
  },
  {
    name: "volume_1",
    url: GECKO_BASE +
      "/networks/solana/pools?page=1&sort=h24_volume_usd_desc&include=base_token,quote_token,dex"
  }
];

// --------------------------------------------------
// VARIABLES
// --------------------------------------------------

let running = true;
let scanning = false;
let scannerState = "STARTING";

let pools = [];
let lastUpdate = null;
let lastError = null;

let geckoCooldownUntil = 0;
let jupiterCooldownUntil = 0;

let geckoLastRequestAt = 0;
let geckoLastCheck = null;
let geckoLastError = null;
let geckoCallsCount = 0;
let geckoSuccessCount = 0;
let geckoFailureCount = 0;
let geckoBackoffLevel = 0;
let geckoFeedIndex = 0;
let geckoCurrentFeed = null;
let geckoInFlight = null;

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

let nextScanAt = Date.now();
let nextScanInSeconds = 0;

let heliusLastSlot = null;
let heliusLastCheck = null;
let heliusError = null;

const geckoCache = new Map();
const poolCatalog = new Map();
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

function isGeckoCooldown() {
  return Date.now() < geckoCooldownUntil;
}

function isJupiterCooldown() {
  return Date.now() < jupiterCooldownUntil;
}

function retryAfterMs(response, fallback) {
  const value = response.headers.get("retry-after");

  if (!value) return fallback;

  const seconds = Number(value);

  if (Number.isFinite(seconds) && seconds > 0) {
    return Math.max(1000, seconds * 1000);
  }

  const date = Date.parse(value);

  if (Number.isFinite(date)) {
    return Math.max(1000, date - Date.now());
  }

  return fallback;
}

function setGeckoCooldown(ms) {
  geckoCooldownUntil = Math.max(
    geckoCooldownUntil,
    Date.now() + ms
  );
}

function setJupiterCooldown(ms = CONFIG.JUPITER_COOLDOWN_MS) {
  jupiterCooldownUntil = Math.max(
    jupiterCooldownUntil,
    Date.now() + ms
  );
}

// --------------------------------------------------
// REQUÊTES HTTP
// --------------------------------------------------

async function httpJson(url, options = {}, timeoutMs = CONFIG.REQUEST_TIMEOUT) {
  const controller = new AbortController();

  const timeout = setTimeout(
    () => controller.abort(),
    timeoutMs
  );

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });

    if (!response.ok) {
      const error = new Error("HTTP " + response.status);

      error.status = response.status;
      error.retryAfterMs = retryAfterMs(response, 60000);

      throw error;
    }

    return await response.json();

  } finally {
    clearTimeout(timeout);
  }
}

// --------------------------------------------------
// GECKOTERMINAL
// --------------------------------------------------

function geckoBackoffMs() {
  const delays = [
    120000,
    300000,
    600000,
    1200000,
    CONFIG.GECKO_MAX_COOLDOWN_MS
  ];

  return delays[
    Math.min(geckoBackoffLevel, delays.length - 1)
  ];
}

async function geckoRequest(feed) {
  if (geckoInFlight) {
    return geckoInFlight;
  }

  const cached = geckoCache.get(feed.name);

  if (
    cached &&
    Date.now() - cached.time < CONFIG.GECKO_CACHE_MS
  ) {
    return cached.data;
  }

  if (isGeckoCooldown()) {
    if (
      cached &&
      Date.now() - cached.time < CONFIG.GECKO_STALE_MAX_MS
    ) {
      return cached.data;
    }

    throw new Error("GeckoTerminal en pause");
  }

  geckoInFlight = (async () => {
    const wait = Math.max(
      0,
      CONFIG.GECKO_REQUEST_GAP_MS -
        (Date.now() - geckoLastRequestAt)
    );

    if (wait > 0) await sleep(wait);

    if (isGeckoCooldown()) {
      throw new Error("GeckoTerminal en pause");
    }

    geckoLastRequestAt = Date.now();
    geckoLastCheck = new Date().toISOString();
    geckoCurrentFeed = feed.name;
    geckoCallsCount++;

    try {
      const data = await httpJson(
        feed.url,
        {
          headers: {
            Accept: "application/json;version=20230302"
          }
        }
      );

      if (!data || !Array.isArray(data.data)) {
        throw new Error("Format GeckoTerminal invalide");
      }

      if (data.data.length > 0) {
        geckoCache.set(feed.name, {
          data,
          time: Date.now()
        });
      }

      geckoBackoffLevel = 0;
      geckoLastError = null;
      geckoSuccessCount++;

      return data;

    } catch (error) {
      geckoFailureCount++;

      if (error.status === 429) {
        const pause = error.retryAfterMs ||
          geckoBackoffMs();

        setGeckoCooldown(pause);
        geckoBackoffLevel++;

        geckoLastError = "GeckoTerminal HTTP 429";
      } else {
        geckoLastError = error.message;
      }

      if (
        cached &&
        Date.now() - cached.time < CONFIG.GECKO_STALE_MAX_MS
      ) {
        return cached.data;
      }

      throw error;
    }
  })();

  try {
    return await geckoInFlight;
  } finally {
    geckoInFlight = null;
  }
}

// --------------------------------------------------
// NORMALISATION GECKOTERMINAL
// --------------------------------------------------

function relationshipAddress(relationship) {
  const id =
    relationship &&
    relationship.data &&
    relationship.data.id;

  if (!id) return null;

  const separator = id.indexOf("_");

  return separator >= 0
    ? id.slice(separator + 1)
    : id;
}

function normalizeGeckoResponse(response) {
  const included = new Map();

  for (const item of response.included || []) {
    if (item.type === "token" && item.attributes) {
      const address =
        item.attributes.address ||
        relationshipAddress({
          data: { id: item.id }
        });

      if (address) {
        included.set(address, {
          address,
          name: item.attributes.name || "Token Solana",
          symbol: item.attributes.symbol || "UNKNOWN",
          decimals: item.attributes.decimals
        });
      }
    }
  }

  const normalized = [];

  const quoteMints = new Set([
    SOL_MINT,
    USDC_MINT,
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
  ]);

  for (const item of response.data || []) {
    if (!item.attributes) continue;

    const a = item.attributes;
    const rel = item.relationships || {};

    const baseAddress =
      relationshipAddress(rel.base_token);

    const quoteAddress =
      relationshipAddress(rel.quote_token);

    if (!baseAddress || !quoteAddress) continue;

    const baseInfo = included.get(baseAddress) || {
      address: baseAddress,
      name: "Token Solana",
      symbol: "UNKNOWN"
    };

    const quoteInfo = included.get(quoteAddress) || {
      address: quoteAddress,
      name: "Token Solana",
      symbol: "UNKNOWN"
    };

    let token = baseInfo;
    let otherToken = quoteInfo;
    let tokenPrice = num(a.base_token_price_usd);

    if (
      quoteMints.has(baseAddress) &&
      !quoteMints.has(quoteAddress)
    ) {
      token = quoteInfo;
      otherToken = baseInfo;
      tokenPrice = num(a.quote_token_price_usd);
    }

    if (!token.address || quoteMints.has(token.address)) {
      continue;
    }

    const createdAt = a.pool_created_at
      ? Date.parse(a.pool_created_at)
      : 0;

    const tx = a.transactions || {};
    const vol = a.volume_usd || {};
    const changes = a.price_change_percentage || {};

    const dexRelationship =
      rel.dex &&
      rel.dex.data &&
      rel.dex.data.id;

    const poolAddress = a.address;

    if (!poolAddress) continue;

    normalized.push({
      chainId: "solana",

      address: token.address,
      pairAddress: poolAddress,

      baseToken: token,
      quoteToken: otherToken,

      dexId: dexRelationship || "geckoterminal",

      name: token.name,
      symbol: token.symbol,

      priceUsd: tokenPrice,

      liquidity: {
        usd: num(a.reserve_in_usd)
      },

      volume: {
        m5: num(vol.m5),
        h1: num(vol.h1),
        h24: num(vol.h24)
      },

      txns: {
        m5: tx.m5 || {},
        h1: tx.h1 || {},
        h24: tx.h24 || {}
      },

      priceChange: {
        m5: num(changes.m5),
        h1: num(changes.h1),
        h24: num(changes.h24)
      },

      pairCreatedAt: Number.isFinite(createdAt)
        ? createdAt
        : 0,

      url:
        "https://www.geckoterminal.com/solana/pools/" +
        poolAddress
    });
  }

  return normalized;
}

// --------------------------------------------------
// CATALOGUE CUMULATIF DES POOLS
// --------------------------------------------------

function updatePoolCatalog(pairs) {
  const now = Date.now();

  for (const pair of pairs) {
    const key = pair.pairAddress;

    if (!key) continue;

    const previous = poolCatalog.get(key);

    const currentLiquidity = num(
      pair.liquidity && pair.liquidity.usd
    );

    const previousLiquidity = previous
      ? num(previous.pair.liquidity && previous.pair.liquidity.usd)
      : 0;

    // On conserve la meilleure paire connue pour ce pool.
    if (
      !previous ||
      currentLiquidity >= previousLiquidity
    ) {
      poolCatalog.set(key, {
        pair,
        updatedAt: now
      });
    } else {
      previous.updatedAt = now;
    }
  }

  for (const [key, value] of poolCatalog.entries()) {
    if (now - value.updatedAt > CONFIG.CATALOG_TTL_MS) {
      poolCatalog.delete(key);
    }
  }

  // Protection contre une croissance illimitée.
  if (poolCatalog.size > CONFIG.MAX_CATALOG_POOLS) {
    const sorted = Array.from(poolCatalog.entries())
      .sort((a, b) => b[1].updatedAt - a[1].updatedAt);

    poolCatalog.clear();

    for (const [key, value] of sorted.slice(
      0,
      CONFIG.MAX_CATALOG_POOLS
    )) {
      poolCatalog.set(key, value);
    }
  }
}

function getCatalogPairs() {
  const grouped = new Map();

  for (const value of poolCatalog.values()) {
    const pair = value.pair;

    if (!pair || !pair.address) continue;

    const previous = grouped.get(pair.address);

    if (
      !previous ||
      num(pair.liquidity && pair.liquidity.usd) >
        num(previous.liquidity && previous.liquidity.usd)
    ) {
      grouped.set(pair.address, pair);
    }
  }

  return Array.from(grouped.values());
}

async function discoverPools() {
  const feed = GECKO_FEEDS[
    geckoFeedIndex % GECKO_FEEDS.length
  ];

  geckoFeedIndex =
    (geckoFeedIndex + 1) % GECKO_FEEDS.length;

  try {
    const response = await geckoRequest(feed);
    const normalized = normalizeGeckoResponse(response);

    updatePoolCatalog(normalized);

    geckoLastError = null;

  } catch (error) {
    geckoLastError =
      feed.name + ": " + error.message;
  }

  const catalogPairs = getCatalogPairs();

  if (!catalogPairs.length) {
    throw new Error(
      geckoLastError || "Aucun pool disponible"
    );
  }

  return catalogPairs;
}

// --------------------------------------------------
// HELIUS
// --------------------------------------------------

async function heliusRequest(method, params = []) {
  if (!HELIUS_RPC_URL) {
    throw new Error("HELIUS_RPC_URL non configurée");
  }

  const data = await httpJson(
    HELIUS_RPC_URL,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method,
        params
      })
    }
  );

  if (data.error) {
    throw new Error(
      data.error.message || "Erreur Helius"
    );
  }

  return data.result;
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
// EXTENSIONS TOKEN-2022
// --------------------------------------------------

const DANGEROUS_TOKEN2022_EXTENSIONS = new Set([
  1,  // TransferFeeConfig
  3,  // MintCloseAuthority
  4,  // ConfidentialTransferMint
  6,  // DefaultAccountState
  9,  // NonTransferable
  12, // PermanentDelegate
  14, // TransferHook
  16, // ConfidentialTransferFeeConfig
  25  // PausableConfig, si présent dans la version du programme
]);

function inspectToken2022Extensions(data) {
  if (data.length <= 82) {
    return {
      checked: true,
      extensions: [],
      dangerous: []
    };
  }

  // Les comptes mint Token-2022 avec extensions
  // utilisent une zone de padding puis un type de compte.
  if (data.length < 166 || data.readUInt8(165) !== 1) {
    return {
      checked: false,
      extensions: [],
      dangerous: [],
      error: "Format d'extensions non reconnu"
    };
  }

  let offset = 166;
  const extensions = [];
  const dangerous = [];

  while (offset + 4 <= data.length) {
    const type = data.readUInt16LE(offset);
    const length = data.readUInt16LE(offset + 2);

    offset += 4;

    if (type === 0 && length === 0) break;

    if (offset + length > data.length) {
      return {
        checked: false,
        extensions,
        dangerous,
        error: "TLV Token-2022 invalide"
      };
    }

    extensions.push(type);

    if (DANGEROUS_TOKEN2022_EXTENSIONS.has(type)) {
      dangerous.push(type);
    }

    offset += length;
  }

  return {
    checked: true,
    extensions,
    dangerous
  };
}

// --------------------------------------------------
// SÉCURITÉ DES MINTS
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

  for (let offset = 0; offset < addresses.length; offset += 100) {
    const group = addresses.slice(offset, offset + 100);

    try {
      const response = await heliusRequest(
        "getMultipleAccounts",
        [
          group,
          {
            encoding: "base64",
            commitment: "confirmed"
          }
        ]
      );

      const accounts = response && response.value
        ? response.value
        : [];

      group.forEach((address, index) => {
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

          const initialized =
            data.readUInt8(45) === 1;

          const token2022 =
            owner === TOKEN_2022_PROGRAM;

          const extensionInfo = token2022
            ? inspectToken2022Extensions(data)
            : {
                checked: true,
                extensions: [],
                dangerous: []
              };

          results.set(address, {
            checked: true,
            owner,
            token2022,

            extensionsChecked: extensionInfo.checked,
            extensions: extensionInfo.extensions,
            dangerousExtensions: extensionInfo.dangerous,
            extensionError: extensionInfo.error || null,

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

      for (const address of group) {
        results.set(address, {
          checked: false,
          error: error.message
        });
      }
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
      const error = new Error("Jupiter en pause");
      error.status = 429;
      throw error;
    }

    const wait = Math.max(
      0,
      CONFIG.JUPITER_REQUEST_GAP_MS -
        (Date.now() - jupiterRequestAt)
    );

    if (wait > 0) await sleep(wait);

    if (isJupiterCooldown()) {
      const error = new Error("Jupiter en pause");
      error.status = 429;
      throw error;
    }

    jupiterRequestAt = Date.now();
  });

  jupiterQueue = task.catch(() => {});

  await task;
}

// --------------------------------------------------
// JUPITER V2
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
      const pause = retryAfterMs(
        response,
        CONFIG.JUPITER_COOLDOWN_MS
      );

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
      throw new Error("Quantité de sortie invalide");
    }

    jupiterLastCheck = new Date().toISOString();

    return data;

  } finally {
    clearTimeout(timeout);
  }
}

// --------------------------------------------------
// PRIX SOL
// --------------------------------------------------

async function getJupiterSolPrice() {
  const cacheValid =
    solPriceCached !== null &&
    Date.now() - solPriceFetchedAt <
      CONFIG.SOL_PRICE_CACHE_MS;

  if (cacheValid) return solPriceCached;

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

  if (raw === undefined || raw === null) return null;

  const value = Number(raw);

  if (!Number.isFinite(value)) return null;

  return value <= 1
    ? Math.abs(value * 100)
    : Math.abs(value);
}

// --------------------------------------------------
// VÉRIFICATION ACHAT + REVENTE
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

    const buy = await jupiterOrder(
      SOL_MINT,
      pool.address,
      String(solAmount)
    );

    const tokenAmount = BigInt(buy.outAmount);

    if (tokenAmount <= 0n) {
      throw new Error("Quantité achetable invalide");
    }

    const sell = await jupiterOrder(
      pool.address,
      SOL_MINT,
      tokenAmount.toString()
    );

    const sellSol = Number(sell.outAmount) / 1e9;
    const initialSol = solAmount / 1e9;

    if (!Number.isFinite(sellSol) || sellSol <= 0) {
      throw new Error("Montant de revente invalide");
    }

    const buyImpact = getPriceImpactPercent(buy);
    const sellImpact = getPriceImpactPercent(sell);

    if (buyImpact === null || sellImpact === null) {
      throw new Error("Impact Jupiter indisponible");
    }

    const maxImpact = Math.max(
      buyImpact,
      sellImpact
    );

    if (maxImpact > CONFIG.MAX_JUPITER_IMPACT_PCT) {
      throw new Error("Impact excessif");
    }

    const returnPercent =
      ((sellSol - initialSol) / initialSol) * 100;

    // Estimation indicative uniquement.
    // Les frais réels dépendent de la transaction.
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

      roundTripReturnPercent: round(returnPercent, 3),
      estimatedNetReturnPercent: round(netReturnPercent, 3),

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

      roundTripReturnPercent: round(returnPercent, 3),
      estimatedNetReturnPercent: round(netReturnPercent, 3),

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
// MEILLEURE PAIRE
// --------------------------------------------------

function chooseBestPair(address, pairs) {
  const matching = pairs.filter(pair => {
    return pair && pair.address === address;
  });

  if (!matching.length) return null;

  matching.sort((a, b) => {
    return num(b.liquidity && b.liquidity.usd) -
      num(a.liquidity && a.liquidity.usd);
  });

  return matching[0];
}

// --------------------------------------------------
// ANALYSE ET SCORE
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
    ? Math.max(0, (Date.now() - createdAt) / 60000)
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

    if (mint.token2022 && !mint.extensionsChecked) {
      risks.push("EXTENSIONS NON VÉRIFIÉES");
    }

    if (
      mint.dangerousExtensions &&
      mint.dangerousExtensions.length
    ) {
      risks.push("EXTENSION TOKEN-2022 À RISQUE");
    }
  }

  if (totalTransactions < CONFIG.MIN_TRANSACTIONS_1H) {
    risks.push("ACTIVITÉ FAIBLE");
  }

  if (
    totalTransactions >= 5 &&
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

  const volumeLiquidityRatio =
    liquidity > 0 ? volume1h / liquidity : 0;

  if (
    volumeLiquidityRatio >
    CONFIG.MAX_VOLUME_LIQUIDITY_RATIO
  ) {
    risks.push("VOLUME ANORMAL");
  }

  const momentum = num(changes.m5);
  const momentum1h = num(changes.h1);

  // Score rééquilibré sur 100.
  let score = 0;

  score += Math.min(20, liquidity / 1000);
  score += Math.min(20, volume1h / 500);
  score += Math.min(15, totalTransactions / 2);
  score += Math.min(20, buyRatio * 20);
  score += Math.max(0, Math.min(10, momentum));

  score += Math.min(
    5,
    Math.max(0, acceleration - 1) * 2
  );

  if (
    ageMinutes !== null &&
    ageMinutes >= 5 &&
    ageMinutes <= 1440
  ) {
    score += 5;
  }

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
    !mint.freezeAuthorityActive &&
    mint.extensionsChecked &&
    (!mint.dangerousExtensions ||
      mint.dangerousExtensions.length === 0)
  );

  const buyEligible =
    authorityVerified &&
    risks.length === 0;

  const estimatedImpact =
    liquidity > 0
      ? CONFIG.TEST_TRADE_USD / (2 * liquidity) * 100
      : null;

  return {
    address,
    shortAddress: shorten(address),

    name: pair.baseToken.name || "Token Solana",
    symbol: pair.baseToken.symbol || "UNKNOWN",

    dex: pair.dexId || "geckoterminal",
    pairAddress: pair.pairAddress,
    url: pair.url,

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
    volumeLiquidityRatio: round(volumeLiquidityRatio, 3),

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

  scanning = true;
  scannerState = "SCANNING";

  try {
    const pairs = await discoverPools();

    if (!pairs.length) {
      throw new Error("Aucun pool exploitable trouvé");
    }

    // Priorité aux pools les plus liquides.
    const sortedPairs = [...pairs].sort((a, b) => {
      return num(b.liquidity && b.liquidity.usd) -
        num(a.liquidity && a.liquidity.usd);
    });

    const uniqueAddresses = Array.from(
      new Set(sortedPairs.map(pair => pair.address))
    ).slice(0, CONFIG.MAX_POOLS);

    const mintResults =
      await checkMintAuthorities(uniqueAddresses);

    const results = [];

    for (const address of uniqueAddresses) {
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

    results.sort((a, b) => b.score - a.score);

    jupiterChecksCount = 0;
    jupiterSuccessCount = 0;

    const candidates = results
      .filter(pool => pool.buyEligible)
      .slice(0, CONFIG.MAX_JUPITER_CHECKS);

    if (!JUPITER_API_KEY) {
      jupiterLastError = "Clé API non configurée";

      for (const pool of candidates) {
        pool.buyEligible = false;
        pool.sellability = "JUPITER NON CONFIGURÉ";

        pool.jupiter = {
          verified: false,
          error: "JUPITER_API_KEY non configurée"
        };
      }

    } else if (candidates.length > 0) {
      try {
        const solPrice = await getJupiterSolPrice();

        for (const pool of candidates) {
          if (isJupiterCooldown()) {
            pool.buyEligible = false;
            pool.sellability = "JUPITER EN PAUSE";

            pool.jupiter = {
              verified: false,
              error: "Jupiter en pause"
            };

            break;
          }

          const success =
            await checkJupiterSellability(pool, solPrice);

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

    } else {
      jupiterLastError =
        "Aucun candidat ne passe les filtres préalables";
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

    scannerState = geckoLastError
      ? "DEGRADED"
      : "OK";

    for (const [key, value] of snapshots.entries()) {
      if (Date.now() - value.time > 3600000) {
        snapshots.delete(key);
      }
    }

  } catch (error) {
    lastError = error.message;

    scannerState = "DEGRADED";

    // Les anciens pools ne sont jamais effacés
    // simplement parce qu'une API est indisponible.
  } finally {
    scanning = false;

    const cooldownRemaining = Math.max(
      geckoCooldownUntil - Date.now(),
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
// PROGRAMMATION
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
// DÉMARRAGE / ARRÊT
// --------------------------------------------------

function startScanner() {
  if (running && scanning) return;

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
    nextScanInSeconds = Math.max(
      0,
      Math.ceil((nextScanAt - Date.now()) / 1000)
    );
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
    version: "GECKO-V5",
    mode: "TEST",
    realTrading: false
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    running,
    scanner: scannerState,
    realTrading: false
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
    catalogPools: poolCatalog.size,

    lastUpdate,
    lastError,

    scanning,
    nextScanInSeconds,

    discovery: {
      provider: "GECKOTERMINAL",
      configured: true,

      feeds: GECKO_FEEDS.length,
      currentFeed: geckoCurrentFeed,
      nextFeed: GECKO_FEEDS[geckoFeedIndex].name,

      callsCount: geckoCallsCount,
      successCount: geckoSuccessCount,
      failureCount: geckoFailureCount,

      lastCheck: geckoLastCheck,
      lastError: geckoLastError,

      backoffLevel: geckoBackoffLevel,

      cooldownSeconds: Math.max(
        0,
        Math.ceil(
          (geckoCooldownUntil - Date.now()) / 1000
        )
      ),

      cachedFeeds: Array.from(geckoCache.keys())
    },

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

    protections: {
      mintAuthorityCheck: true,
      freezeAuthorityCheck: true,
      tokenProgramCheck: true,

      token2022ExtensionInspection: true,

      liquidityCheck: true,
      volumeCheck: true,
      suspiciousActivityCheck: true,

      buyPressureCheck: true,
      scoreCheck: true,

      buyQuoteCheck: true,
      sellQuoteCheck: true,

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
    catalogPools: poolCatalog.size,
    maxPools: CONFIG.MAX_POOLS,

    mode: "TEST",
    realTrading: false,

    pools
  });
});

// --------------------------------------------------
// SÉCURITÉ
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

    catalogPools: poolCatalog.size,

    discovery: {
      provider: "GECKOTERMINAL",
      lastError: geckoLastError,
      currentFeed: geckoCurrentFeed
    },

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

      ageMinutes: p.ageMinutes,
      volumeLiquidityRatio: p.volumeLiquidityRatio,

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
// TEST GECKOTERMINAL
// Ne déclenche pas de nouvel appel API.
// --------------------------------------------------

app.get("/gecko-test", (req, res) => {
  res.json({
    ok: !isGeckoCooldown(),
    provider: "GECKOTERMINAL",
    currentFeed: geckoCurrentFeed,
    catalogPools: poolCatalog.size,
    callsCount: geckoCallsCount,
    lastCheck: geckoLastCheck,
    lastError: geckoLastError,

    cooldownSeconds: Math.max(
      0,
      Math.ceil(
        (geckoCooldownUntil - Date.now()) / 1000
      )
    )
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

    jupiterLastCheck = new Date().toISOString();
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
// CONTRÔLES SCANNER
// --------------------------------------------------

app.post("/test/start", (req, res) => {
  if (!running) {
    startScanner();
  }

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
  console.log("CART3R.JAILBREAK GECKO V5 démarré");
  console.log("Découverte : GECKOTERMINAL");
  console.log("Rotation : un flux par scan");
  console.log("Maximum : 100 pools");
  console.log("Catalogue cumulatif : 300 pools maximum");

  console.log(
    "HELIUS : " +
    (HELIUS_RPC_URL ? "CONFIGURÉ" : "ABSENT")
  );

  console.log(
    "JUPITER V2 : " +
    (JUPITER_API_KEY ? "CONFIGURÉ" : "ABSENT")
  );

  console.log("MODE TEST");
  console.log("TRADING RÉEL DÉSACTIVÉ");

  startScanner();
});
