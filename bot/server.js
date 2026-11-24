
"use strict";

const express = require("express");
const app = express();

app.use(express.json());

const PORT = process.env.PORT || 3000;

const HELIUS_RPC_URL = process.env.HELIUS_RPC_URL || "";
const JUPITER_API_KEY = process.env.JUPITER_API_KEY || "";

const SOL_MINT = "So11111111111111111111111111111111111111112";
const USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

const CONFIG = {
  MAX_POOLS: 30,
  MAX_JUPITER_CHECKS: 8,

  SCAN_INTERVAL: 300000,
  ADDRESSES_CACHE_MS: 1800000,
  PAIRS_CACHE_MS: 300000,
  COOLDOWN_MS: 300000,
  REQUEST_TIMEOUT: 15000,

  JUPITER_REQUEST_GAP_MS: 250,

  MIN_LIQUIDITY_USD: 3000,
  MIN_VOLUME_1H_USD: 500,
  MIN_AGE_MINUTES: 5,

  TEST_TRADE_USD: 10,
  MAX_JUPITER_IMPACT_PCT: 5,

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

let cooldownUntil = 0;
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

let scanTimer = null;
let heliusTimer = null;
let countdownTimer = null;

let nextScanInSeconds = 0;

let cachedAddresses = [];
let addressesFetchedAt = 0;

let cachedPairs = [];
let pairsFetchedAt = 0;

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
      cooldownUntil = Date.now() + CONFIG.COOLDOWN_MS;

      const error = new Error(
        "Limite DEX Screener : pause de 300 secondes"
      );

      error.status = 429;
      throw error;
    }

    if (!response.ok) {
      throw new Error("DEX Screener HTTP " + response.status);
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
// VERIFICATION DES AUTORITES MINT
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
    const response = await heliusRequest(
      "getMultipleAccounts",
      [
        addresses,
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
        const data = Buffer.from(
          account.data[0],
          "base64"
        );

        if (data.length < 82) {
          throw new Error("Données mint invalides");
        }

        results.set(address, {
          checked: true,
          initialized: data.readUInt8(45) === 1,

          mintAuthorityActive:
            data.readUInt32LE(0) !== 0,

          freezeAuthorityActive:
            data.readUInt32LE(46) !== 0,

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
// JUPITER V2
// COTATIONS ACHAT ET REVENTE
// --------------------------------------------------

async function jupiterOrder(inputMint, outputMint, amount) {
  if (!JUPITER_API_KEY) {
    throw new Error("JUPITER_API_KEY non configurée");
  }

  const wait = Math.max(
    0,
    CONFIG.JUPITER_REQUEST_GAP_MS -
      (Date.now() - jupiterRequestAt)
  );

  if (wait > 0) {
    await sleep(wait);
  }

  jupiterRequestAt = Date.now();

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
      throw new Error(
        "Réponse Jupiter invalide"
      );
    }

    if (!response.ok) {
      throw new Error(
        data.errorMessage ||
        data.error ||
        "Jupiter HTTP " + response.status
      );
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
      throw new Error(
        "Aucune quantité de sortie Jupiter"
      );
    }

    return data;

  } finally {
    clearTimeout(timeout);
  }
}

// --------------------------------------------------
// PRIX SOL VIA JUPITER
// --------------------------------------------------

async function getJupiterSolPrice() {
  const quote = await jupiterOrder(
    SOL_MINT,
    USDC_MINT,
    "1000000000"
  );

  const price = Number(quote.outAmount) / 1000000;

  if (!Number.isFinite(price) || price <= 0) {
    throw new Error("Prix SOL invalide");
  }

  jupiterLastSolPriceUsd = round(price, 4);

  return price;
}

// --------------------------------------------------
// EXTRACTION DE L'IMPACT DE PRIX
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

  // Les valeurs décimales Jupiter sont converties
  // en pourcentage.
  return value <= 1
    ? Math.abs(value * 100)
    : Math.abs(value);
}

// --------------------------------------------------
// VERIFICATION ACHAT + REVENTE JUPITER
// --------------------------------------------------

async function checkJupiterSellability(pool, solPrice) {
  const checkedAt = new Date().toISOString();

  try {
    // Montant de test en SOL
    const solAmount = Math.floor(
      CONFIG.TEST_TRADE_USD / solPrice * 1e9
    );

    if (solAmount <= 0) {
      throw new Error("Montant SOL invalide");
    }

    // 1. COTATION ACHAT : SOL -> TOKEN

    const buy = await jupiterOrder(
      SOL_MINT,
      pool.address,
      String(solAmount)
    );

    const tokenAmount = BigInt(buy.outAmount);

    if (tokenAmount <= 0n) {
      throw new Error("Quantité achetable invalide");
    }

    // 2. COTATION VENTE : TOKEN -> SOL
    // On teste exactement le montant obtenu à l'achat.

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

    // Rendement théorique achat puis revente

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

    // Protection contre un impact excessif

    if (
      maxImpact !== null &&
      maxImpact > CONFIG.MAX_JUPITER_IMPACT_PCT
    ) {
      throw new Error(
        "Impact de prix trop élevé : " +
        round(maxImpact, 3) + "%"
      );
    }

    // Résultat positif

    pool.sellability = "COTATION ACHAT + VENTE OK";

    pool.jupiter = {
      verified: true,
      checkedAt,

      routerBuy: buy.router || null,
      routerSell: sell.router || null,

      buyAmount: buy.inAmount,
      tokenAmount: buy.outAmount,
      sellOutputSol: sell.outAmount,

      buyFeeBps: buy.feeBps ?? null,
      sellFeeBps: sell.feeBps ?? null,

      buyPriceImpactPercent:
        buyImpact === null
          ? null
          : round(buyImpact, 4),

      sellPriceImpactPercent:
        sellImpact === null
          ? null
          : round(sellImpact, 4),

      maxPriceImpactPercent:
        maxImpact === null
          ? null
          : round(maxImpact, 4),

      roundTripReturnPercent:
        round(returnPercent, 3),

      quoteOnly: true
    };

    pool.simulation = {
      ...pool.simulation,

      tradeAmountUsd: CONFIG.TEST_TRADE_USD,
      tradeAmountSol: round(initialSol, 9),

      estimatedSellValueUsd:
        round(sellSol * solPrice),

      roundTripReturnPercent:
        round(returnPercent, 3),

      realQuote: true,
      quoteOnly: true,

      executionPerformed: false
    };

    pool.buyEligible = true;

    jupiterSuccessCount++;

    return true;

  } catch (error) {
    pool.sellability = "VENTE NON VALIDÉE";
    pool.buyEligible = false;

    pool.jupiter = {
      verified: false,
      checkedAt,
      error: error.message
    };

    pool.simulation = {
      ...pool.simulation,
      realQuote: false,
      executionPerformed: false
    };

    jupiterLastError = error.message;

    return false;
  }
}

// --------------------------------------------------
// CACHE DES ADRESSES
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

      if (addresses.length >= CONFIG.MAX_POOLS) {
        break;
      }
    }

    if (!addresses.length) {
      throw new Error("Aucun token Solana trouvé");
    }

    cachedAddresses = addresses;
    addressesFetchedAt = Date.now();

    return cachedAddresses;

  } catch (error) {
    if (cachedAddresses.length > 0) {
      console.log("Utilisation du cache des tokens");
      return cachedAddresses;
    }

    throw error;
  }
}

// --------------------------------------------------
// CACHE DES PAIRES
// --------------------------------------------------

async function getPairs(addresses) {
  const cacheValid =
    cachedPairs.length > 0 &&
    Date.now() - pairsFetchedAt <
      CONFIG.PAIRS_CACHE_MS;

  if (cacheValid) {
    return cachedPairs;
  }

  try {
    const allPairs = [];

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

    return cachedPairs;

  } catch (error) {
    if (cachedPairs.length > 0) {
      console.log("Utilisation du cache des paires");
      return cachedPairs;
    }

    throw error;
  }
}

// --------------------------------------------------
// SELECTION DES PAIRES
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

  matching.sort((a, b) =>
    num(b.liquidity && b.liquidity.usd) -
    num(a.liquidity && a.liquidity.usd)
  );

  return matching[0];
}

// --------------------------------------------------
// ANALYSE ET SECURITE
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

  if (
    ageMinutes !== null &&
    ageMinutes < CONFIG.MIN_AGE_MINUTES
  ) {
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

  if (totalTransactions < 5) {
    risks.push("ACTIVITÉ FAIBLE");
  }

  if (liquidity > 0 && volume1h > liquidity * 10) {
    risks.push("VOLUME ANORMAL");
  }

  const momentum = num(changes.m5);

  let score = 0;

  score += Math.min(25, liquidity / 1000);
  score += Math.min(25, volume1h / 1000);
  score += Math.min(15, totalTransactions / 4);
  score += Math.min(15, buyRatio * 15);
  score += Math.max(0, Math.min(10, momentum));

  score += Math.min(
    10,
    Math.max(0, acceleration - 1) * 5
  );

  score = Math.round(Math.min(100, score));

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

    change5m: round(momentum),
    change1h: round(num(changes.h1)),

    ageMinutes: ageMinutes === null
      ? null
      : round(ageMinutes),

    acceleration,
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

  if (Date.now() < cooldownUntil) {
    scannerState = "COOLDOWN";
    return;
  }

  scanning = true;
  scannerState = "SCANNING";

  try {
    const addresses = await getAddresses();

    const pairs = await getPairs(addresses);

    // Helius indépendant de DexScreener

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

    // Classement initial

    results.sort((a, b) => b.score - a.score);

    // ------------------------------------------------
    // CONTROLE JUPITER
    // ------------------------------------------------

    jupiterChecksCount = 0;
    jupiterSuccessCount = 0;
    jupiterLastError = null;

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
        // Vérification du prix SOL

        const solPrice = await getJupiterSolPrice();

        for (const pool of candidates) {
          await checkJupiterSellability(
            pool,
            solPrice
          );

          jupiterChecksCount++;
        }

        jupiterLastCheck =
          new Date().toISOString();

      } catch (error) {
        jupiterLastError = error.message;

        for (const pool of candidates) {
          pool.buyEligible = false;

          pool.sellability =
            "JUPITER NON DISPONIBLE";

          pool.jupiter = {
            verified: false,
            error: error.message
          };
        }
      }
    }

    // ------------------------------------------------
    // CLASSEMENT FINAL
    // ------------------------------------------------

    results.sort((a, b) => {
      if (a.buyEligible !== b.buyEligible) {
        return a.buyEligible ? -1 : 1;
      }

      return b.score - a.score;
    });

    // Ne pas effacer les anciennes données
    // si le scan échoue.

    pools = results;

    lastUpdate = new Date().toISOString();
    lastError = null;
    scannerState = "OK";

    // Nettoyage des anciennes statistiques

    for (const [key, value] of snapshots.entries()) {
      if (Date.now() - value.time > 3600000) {
        snapshots.delete(key);
      }
    }

  } catch (error) {
    lastError = error.message;

    if (error.status === 429) {
      cooldownUntil =
        Date.now() + CONFIG.COOLDOWN_MS;

      scannerState = "COOLDOWN";
    } else {
      scannerState = "ERROR";
    }

  } finally {
    scanning = false;

    nextScanAt = Math.max(
      Date.now() + 1000,
      cooldownUntil,
      Date.now() + CONFIG.SCAN_INTERVAL
    );
  }
}

// --------------------------------------------------
// DEMARRAGE ET ARRET
// --------------------------------------------------

function startScanner() {
  if (scanTimer) clearInterval(scanTimer);
  if (heliusTimer) clearInterval(heliusTimer);
  if (countdownTimer) clearInterval(countdownTimer);

  running = true;
  nextScanAt = Date.now();

  scan();
  checkHelius();

  scanTimer = setInterval(() => {
    if (Date.now() >= cooldownUntil) {
      scan();
    }
  }, CONFIG.SCAN_INTERVAL);

  heliusTimer = setInterval(() => {
    checkHelius();
  }, 300000);

  countdownTimer = setInterval(() => {
    if (cooldownUntil > Date.now()) {
      nextScanInSeconds = Math.ceil(
        (cooldownUntil - Date.now()) / 1000
      );
    } else {
      nextScanInSeconds = Math.max(
        0,
        Math.ceil(
          (nextScanAt - Date.now()) / 1000
        )
      );
    }
  }, 1000);
}

function stopScanner() {
  running = false;

  clearInterval(scanTimer);
  clearInterval(heliusTimer);
  clearInterval(countdownTimer);

  scanTimer = null;
  heliusTimer = null;
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
      liquidityCheck: true,
      volumeCheck: true,
      suspiciousActivityCheck: true,

      sellabilityQuoteCheck: true,

      // Une cotation n'est pas une simulation
      // de transaction on-chain.
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
      lastError: jupiterLastError
    },

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
// CONTROLES DU SCANNER
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
  console.log("CART3R.JAILBREAK démarré");
  console.log("30 POOLS MAXIMUM");
  console.log("HELIUS : " +
    (HELIUS_RPC_URL ? "CONFIGURÉ" : "ABSENT"));

  console.log("JUPITER V2 : " +
    (JUPITER_API_KEY ? "CONFIGURÉ" : "ABSENT"));

  console.log("MODE TEST - TRADING RÉEL DÉSACTIVÉ");

  startScanner();
});
