"use strict";

/**
 * CART3R.JAILBREAK — Helius on-chain scanner (TEST ONLY)
 * - No GeckoTerminal, no Pump.fun feed, no wallet signing, no transactions sent.
 * - Uses standard Helius logsSubscribe WebSockets (one mention subscription per DEX program).
 * - Fetches confirmed swap transactions and derives observed wallet token deltas.
 * - Uses Jupiter Swap V2 order quotes only to test a small buy -> sell round trip.
 *
 * Important: activity/volume below is ONLY what this process has observed since it started.
 * Verified CPMM quote-side reserves are read on-chain; estimated total liquidity is labeled as an estimate.
 * LaunchLab curve-only candidates remain ineligible until a verified Raydium CPMM pool is observed.
 */

const express = require("express");
const crypto = require("crypto");
const app = express();
app.use(express.json({ limit: "100kb" }));
// Public read-only CORS for the GitHub Pages dashboard.
// Only GET/OPTIONS are allowed; this does not expose trading or write endpoints.
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

const PORT = Number(process.env.PORT || 3000);
const HELIUS_RPC_URL = String(process.env.HELIUS_RPC_URL || "").trim();
const JUPITER_API_KEY = String(process.env.JUPITER_API_KEY || "").trim();

const SOL_MINT = "So11111111111111111111111111111111111111112";
const USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
const TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const RAYDIUM_CPMM_PROGRAM = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";

// Official mainnet program addresses. We listen for actual transactions, not market-indexer data.
const DEX_PROGRAMS = [
  // Focus on new-token launch activity and Raydium's constant-product pools.
  { name: "Raydium LaunchLab", id: "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj" },
  { name: "Raydium CPMM", id: "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C" }
];

const CONFIG = {
  maxCandidates: 100,
  windowMs: 15 * 60 * 1000,
  checkEveryMs: 45 * 1000,
  checkBatch: 5,
  minSwaps: 5,
  minBuyers: 2,
  minBuyPressure: 0.55,
  maxRoundTripLossPct: 8,
  buyTestLamports: "10000000", // 0.01 SOL — quote only, no order execution
  maxSeenSignatures: 5000,
  maxQueuedSignatures: 500,
  maxConcurrentTransactions: 2,
  minLiquiditySol: 5,
  quoteLadderLamports: ["10000000", "50000000"], // 0.01 and 0.05 SOL, quote-only
  rpcTimeoutMs: 12000,
  jupiterTimeoutMs: 12000
};

const state = {
  running: true,
  mode: "TEST",
  realTrading: false,
  scanner: "STARTING",
  pools: new Map(),
  seenSignatures: new Set(),
  signatureQueue: [],
  processing: 0,
  ws: null,
  wsReconnectTimer: null,
  checkTimer: null,
  lastUpdate: null,
  lastError: null,
  scanning: false,
  nextScanInSeconds: 0,
  startedAt: new Date().toISOString(),
  helius: {
    configured: Boolean(HELIUS_RPC_URL),
    connected: false,
    subscriptions: 0,
    lastCheck: null,
    lastSlot: null,
    lastError: null,
    transactionsSeen: 0,
    swapsDecoded: 0,
    reconnects: 0,
    rpc429Retries: 0,
    droppedQueuedTransactions: 0
  },
  jupiter: {
    configured: Boolean(JUPITER_API_KEY),
    api: "SWAP_V2_ORDER",
    lastCheck: null,
    lastRequestAt: null,
    lastSolPriceUsd: null,
    lastSolPriceAt: 0,
    checksCount: 0,
    successCount: 0,
    rateLimitHits: 0,
    cooldownSeconds: 0,
    lastError: null
  },
  discovery: {
    provider: "HELIUS_LOGS_WEBSOCKET",
    feeds: DEX_PROGRAMS.length,
    currentFeed: null,
    callsCount: 0,
    successCount: 0,
    failureCount: 0,
    lastCheck: null,
    lastError: null
  },
  protections: {
    mintAuthorityCheck: true,
    freezeAuthorityCheck: true,
    tokenProgramCheck: true,
    token2022ExtensionInspection: false,
    poolAddressDiscovery: true,
    liquidityCheck: true,
    observedSwapActivityCheck: true,
    buyPressureCheck: true,
    volumePressureCheck: true,
    scoreCheck: true,
    buyQuoteCheck: true,
    sellQuoteCheck: true,
    quoteLadderCheck: true,
    sellabilitySimulation: false,
    realTrading: false
  }
};

const now = () => Date.now();
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const toNum = v => Number.isFinite(Number(v)) ? Number(v) : 0;
const round = (n, d = 2) => Number.isFinite(n) ? Number(n.toFixed(d)) : null;
const safeError = e => String(e && e.message ? e.message : e).slice(0, 350);

function rpcWebSocketUrl() {
  if (!HELIUS_RPC_URL) return null;
  try {
    const u = new URL(HELIUS_RPC_URL);
    u.protocol = "wss:";
    // Helius standard mainnet RPC URLs work as the matching websocket endpoint.
    if (!u.hostname.includes("helius")) {
      const key = u.searchParams.get("api-key");
      const fallback = new URL("wss://mainnet.helius-rpc.com/");
      if (key) fallback.searchParams.set("api-key", key);
      return fallback.toString();
    }
    return u.toString();
  } catch (_) {
    return null;
  }
}

async function fetchJson(url, options = {}, timeoutMs = CONFIG.rpcTimeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    const body = await response.text();
    let json = {};
    try { json = body ? JSON.parse(body) : {}; } catch (_) {}
    if (!response.ok) {
      const detail = json.error?.message || body.slice(0, 160) || response.statusText;
        const error = new Error("HTTP " + response.status + ": " + detail);
      error.status = response.status;
      const retryAfter = response.headers.get("retry-after");
      const resetHeader = response.headers.get("x-ratelimit-reset");
      error.retryAfterMs = retryAfter
        ? (Number.isFinite(Number(retryAfter)) ? Number(retryAfter) * 1000 : Math.max(0, Date.parse(retryAfter) - Date.now()))
        : 0;
      error.rateLimitResetAt = resetHeader && Number.isFinite(Number(resetHeader))
        ? Number(resetHeader) * 1000
        : 0;
      error.rateLimitRemaining = Number(response.headers.get("x-ratelimit-remaining"));
      throw error;
    }
    if (body && !Object.keys(json).length) throw new Error("Réponse JSON invalide (HTTP " + response.status + ")");
    if (json && typeof json === "object") {
      const resetHeader = response.headers.get("x-ratelimit-reset");
      Object.defineProperty(json, "__rateLimit", {
        value: {
          remaining: Number(response.headers.get("x-ratelimit-remaining")),
          resetAt: resetHeader && Number.isFinite(Number(resetHeader)) ? Number(resetHeader) * 1000 : 0
        },
        enumerable: false
      });
    }
    return json;
  } finally {
    clearTimeout(timer);
  }
}

// Serialize Helius HTTP RPC calls to avoid bursts that trigger HTTP 429.
let heliusRpcTail = Promise.resolve();
let lastHeliusRpcAt = 0;
let heliusCooldownUntil = 0;
async function executeHeliusRpc(method, params) {
  if (!HELIUS_RPC_URL) throw new Error("HELIUS_RPC_URL non configurée");
  for (let attempt = 0; attempt < 4; attempt++) {
    const wait = Math.max(heliusCooldownUntil - Date.now(), 350 - (Date.now() - lastHeliusRpcAt), 0);
    if (wait) await sleep(wait);
    lastHeliusRpcAt = Date.now();
    try {
      const json = await fetchJson(HELIUS_RPC_URL, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: Date.now(), method, params })
      });
      if (json.error) throw new Error(json.error.message || "Erreur RPC Helius");
      return json.result;
    } catch (e) {
      const limited = e?.status === 429 || /429|rate.?limit|too many requests/i.test(safeError(e));
      const transient = e?.status >= 500 && e?.status <= 599;
      if ((!limited && !transient) || attempt === 3) throw e;
      const delay = limited && e?.retryAfterMs > 0 ? Math.min(e.retryAfterMs, 30000) : 1200 * Math.pow(2, attempt);
      if (limited) {
        state.helius.rpc429Retries++;
        heliusCooldownUntil = Math.max(heliusCooldownUntil, Date.now() + delay);
      }
      await sleep(delay);
    }
  }
}
function heliusRpc(method, params) {
  const task = heliusRpcTail.then(
    () => executeHeliusRpc(method, params),
    () => executeHeliusRpc(method, params)
  );
  heliusRpcTail = task.catch(() => {});
  return task;
}

function mintBalanceMap(rows) {
  const map = new Map();
  for (const row of rows || []) {
    if (!row || !row.mint || !row.owner) continue;
    const amount = row.uiTokenAmount && row.uiTokenAmount.uiAmount != null
      ? Number(row.uiTokenAmount.uiAmount)
      : Number(row.uiTokenAmount?.amount || 0) / Math.pow(10, Number(row.uiTokenAmount?.decimals || 0));
    const key = row.owner + "|" + row.mint;
    map.set(key, (map.get(key) || 0) + (Number.isFinite(amount) ? amount : 0));
  }
  return map;
}

function accountKeyObjects(tx) {
  const keys = tx?.transaction?.message?.accountKeys || [];
  const header = tx?.transaction?.message?.header || {};
  const required = Number(header.numRequiredSignatures || 0);
  return keys.map((k, i) => {
    if (typeof k === "string") return { pubkey: k, signer: i < required, index: i };
    return { pubkey: k.pubkey, signer: Boolean(k.signer) || i < required, index: i };
  });
}

// Anchor instruction data is base58. Decode it so pool discovery only accepts
// the two documented CPMM swap instructions, not another 13-account instruction.
function decodeBase58(value) {
  const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
  let number = 0n;
  for (const char of String(value || "")) {
    const digit = alphabet.indexOf(char);
    if (digit < 0) return Buffer.alloc(0);
    number = number * 58n + BigInt(digit);
  }
  const bytes = [];
  while (number > 0n) {
    bytes.unshift(Number(number & 255n));
    number >>= 8n;
  }
  const leadingZeroes = (String(value).match(/^1*/) || [""])[0].length;
  return Buffer.concat([Buffer.alloc(leadingZeroes), Buffer.from(bytes)]);
}
const CPMM_SWAP_DISCRIMINATORS = new Set([
  crypto.createHash("sha256").update("global:swap_base_input").digest().subarray(0, 8).toString("hex"),
  crypto.createHash("sha256").update("global:swap_base_output").digest().subarray(0, 8).toString("hex")
]);

// Raydium CPMM SwapBaseInput/Output uses a documented 13-account layout:
// [3] pool_state, [6] input_vault, [7] output_vault, [10] input_mint, [11] output_mint.
// Only report a pool when a matching swap discriminator and CPMM instruction are present.
function extractRaydiumCpmmMarket(tx, mint) {
  const keys = accountKeyObjects(tx);
  const all = [
    ...(tx?.transaction?.message?.instructions || []),
    ...((tx?.meta?.innerInstructions || []).flatMap(x => x.instructions || []))
  ];
  for (const ix of all) {
    const programId = ix.programId || keys[Number(ix.programIdIndex)]?.pubkey;
    if (programId !== RAYDIUM_CPMM_PROGRAM) continue;
    const data = decodeBase58(ix.data);
    if (data.length < 8 || !CPMM_SWAP_DISCRIMINATORS.has(data.subarray(0, 8).toString("hex"))) continue;
    const accounts = (ix.accounts || []).map(a =>
      typeof a === "number" ? keys[a]?.pubkey : (typeof a === "string" ? a : a?.pubkey)
    );
    if (accounts.length < 13) continue;
    const inputMint = accounts[10];
    const outputMint = accounts[11];
    if (inputMint !== mint && outputMint !== mint) continue;
    const baseIsInput = inputMint === mint;
    const market = {
      poolAddress: accounts[3],
      poolProgramId: RAYDIUM_CPMM_PROGRAM,
      baseVault: baseIsInput ? accounts[6] : accounts[7],
      quoteVault: baseIsInput ? accounts[7] : accounts[6],
      quoteMint: baseIsInput ? outputMint : inputMint
    };
    if (market.poolAddress && market.baseVault && market.quoteVault &&
        [SOL_MINT, USDC_MINT, USDT_MINT].includes(market.quoteMint)) return market;
  }
  return null;
}

function findSwapForSigner(tx) {
  const meta = tx?.meta;
  if (!meta || meta.err) return null;
  const keys = accountKeyObjects(tx);
  const signers = keys.filter(k => k.signer && k.pubkey);
  if (!signers.length) return null;
  const pre = mintBalanceMap(meta.preTokenBalances);
  const post = mintBalanceMap(meta.postTokenBalances);
  const allKeys = new Set([...pre.keys(), ...post.keys()]);
  const solMintSet = new Set([SOL_MINT, USDC_MINT, USDT_MINT]);
  let best = null;

  for (const signer of signers) {
    const deltas = [];
    for (const key of allKeys) {
      const split = key.split("|");
      if (split[0] !== signer.pubkey) continue;
      const mint = split.slice(1).join("|");
      const delta = (post.get(key) || 0) - (pre.get(key) || 0);
      if (Math.abs(delta) > 0) deltas.push({ mint, delta });
    }
    const tokenChanges = deltas.filter(x => !solMintSet.has(x.mint));
    if (!tokenChanges.length) continue;

    const account = meta.preBalances && meta.postBalances ? signer.index : -1;
    let solDelta = 0;
    if (account >= 0 && meta.preBalances[account] != null && meta.postBalances[account] != null) {
      solDelta = (Number(meta.postBalances[account]) - Number(meta.preBalances[account])) / 1e9;
    }
    const stableChange = deltas.find(x => x.mint === USDC_MINT || x.mint === USDT_MINT);
    const wrappedSolChange = deltas.find(x => x.mint === SOL_MINT);
    let quoteDeltaUsd = 0;
    let quoteKind = "SOL";
    if (stableChange) {
      quoteDeltaUsd = stableChange.delta;
      quoteKind = "USD_STABLE";
    } else if (wrappedSolChange) {
      quoteDeltaUsd = wrappedSolChange.delta;
      quoteKind = "SOL";
    } else {
      quoteDeltaUsd = solDelta;
      quoteKind = "SOL";
    }
    if (Math.abs(quoteDeltaUsd) < (quoteKind === "SOL" ? 0.00005 : 0.01)) continue;

    for (const token of tokenChanges) {
      // Buy: wallet receives token while spending SOL/stablecoin. Sell: inverse.
      const direction = token.delta > 0 && quoteDeltaUsd < 0 ? "BUY"
        : token.delta < 0 && quoteDeltaUsd > 0 ? "SELL" : null;
      if (!direction) continue;
      const candidate = {
        mint: token.mint,
        direction,
        tokenAmount: Math.abs(token.delta),
        quoteAmount: Math.abs(quoteDeltaUsd),
        quoteKind,
        trader: signer.pubkey,
        solDelta
      };
      if (!best || candidate.tokenAmount > best.tokenAmount) best = candidate;
    }
  }
  return best;
}

function getOrCreateCandidate(mint, dexName) {
  let p = state.pools.get(mint);
  if (!p) {
    if (state.pools.size >= CONFIG.maxCandidates) {
      const sorted = [...state.pools.values()].sort((a, b) => (a.lastSwapAt || 0) - (b.lastSwapAt || 0));
      const removable = sorted.find(x => !x.eligible);
      if (removable) state.pools.delete(removable.mint);
      else return null;
    }
    p = {
      address: mint,
      mint,
      poolAddress: null,
      poolProgramId: null,
      baseVault: null,
      quoteVault: null,
      quoteMint: null,
      liquidity: null,
      liquidityVerified: false,
      liquiditySol: null,
      quoteReserveSol: null,
      quoteLiquidity: null,
      buyVolumeSolEquivalent: 0,
      sellVolumeSolEquivalent: 0,
      volumeBuyPressure: null,
      name: "Token " + mint.slice(0, 6) + "…" + mint.slice(-4),
      symbol: mint.slice(0, 5).toUpperCase(),
      dex: dexName,
      liquidity: null,
      liquidityVerified: false,
      volume: 0,
      volumeUsd: null,
      volumeSol: 0,
      price: null,
      created: new Date().toISOString(),
      firstSeenAt: now(),
      lastSwapAt: 0,
      swaps: [],
      buyers: new Set(),
      buyCount: 0,
      sellCount: 0,
      buyVolumeSol: 0,
      sellVolumeSol: 0,
      buyVolumeUsd: 0,
      sellVolumeUsd: 0,
      buyPressure: 0,
      score: 0,
      eligible: false,
      risks: ["Analyse en cours"],
      security: { checked: false },
      quotes: { buyOk: false, sellOk: false },
      lastCheckAt: 0,
      checkError: null
    };
    state.pools.set(mint, p);
  }
  if (dexName && !String(p.dex).includes(dexName)) p.dex = String(p.dex).split(", ").concat(dexName).join(", ");
  return p;
}

function recalculateSwapMetrics(p) {
  p.buyers = new Set(p.swaps.filter(x => x.direction === "BUY").map(x => x.trader));
  p.buyCount = p.swaps.filter(x => x.direction === "BUY").length;
  p.sellCount = p.swaps.filter(x => x.direction === "SELL").length;
  p.buyVolumeSol = p.swaps.filter(x => x.direction === "BUY").reduce((s, x) => s + x.solAmount, 0);
  p.sellVolumeSol = p.swaps.filter(x => x.direction === "SELL").reduce((s, x) => s + x.solAmount, 0);
  p.buyVolumeUsd = p.swaps.filter(x => x.direction === "BUY").reduce((s, x) => s + (x.usdAmount || 0), 0);
  p.sellVolumeUsd = p.swaps.filter(x => x.direction === "SELL").reduce((s, x) => s + (x.usdAmount || 0), 0);
  p.buyVolumeSolEquivalent = p.swaps.filter(x => x.direction === "BUY" && x.solEquivalent != null).reduce((s, x) => s + x.solEquivalent, 0);
  p.sellVolumeSolEquivalent = p.swaps.filter(x => x.direction === "SELL" && x.solEquivalent != null).reduce((s, x) => s + x.solEquivalent, 0);
  const total = p.buyCount + p.sellCount;
  p.buyPressure = total ? p.buyCount / total : 0;
  const totalVolumeSol = p.buyVolumeSolEquivalent + p.sellVolumeSolEquivalent;
  p.volumeBuyPressure = totalVolumeSol > 0 ? p.buyVolumeSolEquivalent / totalVolumeSol : null;
  p.volumeSol = p.buyVolumeSol + p.sellVolumeSol;
  p.volumeUsd = p.buyVolumeUsd + p.sellVolumeUsd || null;
  p.volume = p.volumeUsd || p.volumeSol;
}

function addSwap(mint, swap, dexName, signature, slot, market) {
  const p = getOrCreateCandidate(mint, dexName);
  if (!p) return;
  const ts = now();
  const solAmount = swap.quoteKind === "SOL" ? swap.quoteAmount : 0;
  const usdAmount = swap.quoteKind === "USD_STABLE" ? swap.quoteAmount : null;
  const solEquivalent = swap.quoteKind === "SOL"
    ? swap.quoteAmount
    : (state.jupiter.lastSolPriceUsd > 0 ? swap.quoteAmount / state.jupiter.lastSolPriceUsd : null);
  p.swaps.push({ ts, direction: swap.direction, trader: swap.trader, solAmount, usdAmount, solEquivalent, signature, slot });
  p.swaps = p.swaps.filter(x => ts - x.ts <= CONFIG.windowMs);
  if (market) {
    p.poolAddress = market.poolAddress;
    p.poolProgramId = market.poolProgramId;
    p.baseVault = market.baseVault;
    p.quoteVault = market.quoteVault;
    p.quoteMint = market.quoteMint;
    p.liquidityVerified = false;
    p.liquidity = null;
    p.liquiditySol = null;
    p.quoteReserveSol = null;
    p.quoteLiquidity = null;
  }
  p.lastSwapAt = ts;
  recalculateSwapMetrics(p);
  state.lastUpdate = new Date().toISOString();
  state.helius.swapsDecoded++;
}

async function enrichCandidate(p) {
  try {
    const asset = await heliusRpc("getAsset", { id: p.mint });
    const metadata = asset?.content?.metadata || {};
    if (metadata.name) p.name = String(metadata.name).slice(0, 80);
    if (metadata.symbol) p.symbol = String(metadata.symbol).slice(0, 20);
    if (asset?.token_info?.decimals != null) p.decimals = Number(asset.token_info.decimals);
  } catch (_) {
    // Metadata is optional; the mint remains the reliable identifier.
  }
}

async function inspectMint(mint) {
  const info = await heliusRpc("getAccountInfo", [mint, { encoding: "jsonParsed", commitment: "confirmed" }]);
  const value = info && info.value;
  if (!value || !value.owner) throw new Error("Compte mint introuvable");
  const owner = value.owner;
  const parsed = value.data && value.data.parsed && value.data.parsed.info;
  if (!parsed) throw new Error("Données mint non décodées");
  const risks = [];
  if (owner !== TOKEN_PROGRAM && owner !== TOKEN_2022_PROGRAM) risks.push("Programme de token non reconnu");
  if (parsed.isInitialized === false) risks.push("Mint non initialisé");
  if (parsed.mintAuthority) risks.push("Mint authority encore active");
  if (parsed.freezeAuthority) risks.push("Freeze authority encore active");
  // Do not claim to inspect Token-2022 extensions from jsonParsed mint data.
  // For safety, Token-2022 mints are excluded until every extension is explicitly decoded.
  if (owner === TOKEN_2022_PROGRAM) risks.push("Token-2022 bloqué : extensions non vérifiées par ce scanner");
  return {
    checked: true,
    owner,
    tokenProgram: owner === TOKEN_PROGRAM ? "SPL_TOKEN" : owner === TOKEN_2022_PROGRAM ? "TOKEN_2022" : "UNKNOWN",
    decimals: Number(parsed.decimals),
    supply: String(parsed.supply || "0"),
    mintAuthority: parsed.mintAuthority || null,
    freezeAuthority: parsed.freezeAuthority || null,
    risks
  };
}

async function refreshSolPrice() {
  if (state.jupiter.lastSolPriceUsd > 0 && now() - state.jupiter.lastSolPriceAt < 5 * 60 * 1000) return state.jupiter.lastSolPriceUsd;
  const quote = await jupiterOrder(SOL_MINT, USDC_MINT, "1000000000");
  const price = Number(quote.outAmount) / 1e6;
  if (!Number.isFinite(price) || price <= 0) throw new Error("Prix SOL/USDC invalide");
  state.jupiter.lastSolPriceUsd = price;
  state.jupiter.lastSolPriceAt = now();
  state.jupiter.lastCheck = new Date().toISOString();
  return price;
}

// Jupiter Free allows 1 request/second over a 60-second sliding window.
// CART3R deliberately stays below that ceiling and shares ONE queue across
// every Jupiter caller (scanner, SOL price refresh and manual test endpoint).
const JUPITER_MIN_INTERVAL_MS = 1500;
let jupiterRequestTail = Promise.resolve();
let jupiterNextRequestAt = 0;
let jupiterCooldownUntil = 0;
let jupiterQueueDepth = 0;

function queueJupiterRequest(task) {
  jupiterQueueDepth++;
  const run = jupiterRequestTail.then(async () => {
    try {
      const waitUntil = Math.max(jupiterNextRequestAt, jupiterCooldownUntil);
      const waitMs = waitUntil - Date.now();
      if (waitMs > 0) await sleep(waitMs);

      jupiterNextRequestAt = Date.now() + JUPITER_MIN_INTERVAL_MS;
      state.jupiter.lastRequestAt = new Date().toISOString();
      state.jupiter.checksCount++;

      const result = await task();
      state.jupiter.successCount++;

      const rate = result && result.__rateLimit;
      if (rate && rate.remaining === 0 && rate.resetAt > Date.now()) {
        jupiterCooldownUntil = Math.max(jupiterCooldownUntil, rate.resetAt);
      }
      return result;
    } catch (e) {
      if (e && e.status === 429) {
        state.jupiter.rateLimitHits++;
        const resetAt = Number(e.rateLimitResetAt) || 0;
        const retryAfterAt = e.retryAfterMs > 0 ? Date.now() + e.retryAfterMs : 0;
        // Prefer Jupiter's exact sliding-window reset; use a conservative
        // 65-second fallback if the gateway omits its rate-limit headers.
        jupiterCooldownUntil = Math.max(
          jupiterCooldownUntil,
          resetAt,
          retryAfterAt,
          Date.now() + 65000
        );
        state.jupiter.lastError = safeError(e);
      }
      throw e;
    } finally {
      jupiterQueueDepth = Math.max(0, jupiterQueueDepth - 1);
      state.jupiter.cooldownSeconds = Math.max(0, Math.ceil((jupiterCooldownUntil - Date.now()) / 1000));
    }
  });
  jupiterRequestTail = run.catch(() => undefined);
  return run;
}

async function jupiterOrder(inputMint, outputMint, amount) {
  if (!JUPITER_API_KEY) throw new Error("JUPITER_API_KEY non configurée");
  const url = new URL("https://api.jup.ag/swap/v2/order");
  url.searchParams.set("inputMint", inputMint);
  url.searchParams.set("outputMint", outputMint);
  url.searchParams.set("amount", String(amount));
  return queueJupiterRequest(async () => {
    const json = await fetchJson(url.toString(), {
      method: "GET",
      headers: { "accept": "application/json", "x-api-key": JUPITER_API_KEY }
    }, CONFIG.jupiterTimeoutMs);
    if (!json || !json.outAmount || Number(json.outAmount) <= 0) throw new Error("Jupiter ne retourne aucun montant de sortie");
    return json;
  });
}

async function verifyPoolLiquidity(p) {
  if (!p.poolAddress || p.poolProgramId !== RAYDIUM_CPMM_PROGRAM || !p.baseVault || !p.quoteVault) {
    p.liquidityVerified = false;
    return { ok: false, reason: "Pool Raydium CPMM et ses coffres non identifiés" };
  }
  const result = await heliusRpc("getMultipleAccounts", [
    [p.poolAddress, p.baseVault, p.quoteVault],
    { encoding: "jsonParsed", commitment: "confirmed" }
  ]);
  const accounts = result?.value || [];
  if (accounts.length !== 3 || accounts.some(x => !x)) throw new Error("Compte pool ou coffre introuvable");
  const [pool, baseVault, quoteVault] = accounts;
  if (pool.owner !== RAYDIUM_CPMM_PROGRAM) throw new Error("Propriétaire du pool CPMM invalide");
  const baseInfo = baseVault.data?.parsed?.info;
  const quoteInfo = quoteVault.data?.parsed?.info;
  if (!baseInfo || !quoteInfo || baseInfo.mint !== p.mint || quoteInfo.mint !== p.quoteMint) {
    throw new Error("Les coffres ne correspondent pas aux mints attendus");
  }
  const baseAmount = Number(baseInfo.tokenAmount?.uiAmountString ?? baseInfo.tokenAmount?.uiAmount ?? 0);
  const quoteAmount = Number(quoteInfo.tokenAmount?.uiAmountString ?? quoteInfo.tokenAmount?.uiAmount ?? 0);
  if (!Number.isFinite(baseAmount) || !Number.isFinite(quoteAmount) || baseAmount <= 0 || quoteAmount <= 0) {
    throw new Error("Réserves du pool nulles ou invalides");
  }
  let quoteSol = null;
  if (p.quoteMint === SOL_MINT) quoteSol = quoteAmount;
  else if (p.quoteMint === USDC_MINT || p.quoteMint === USDT_MINT) {
    const solPrice = await refreshSolPrice();
    quoteSol = quoteAmount / solPrice;
  }
  if (!Number.isFinite(quoteSol) || quoteSol <= 0) throw new Error("Réserve de cotation non convertible en SOL");
  p.quoteLiquidity = quoteAmount;
  p.quoteReserveSol = round(quoteSol, 4);
  // CPMM reserves are approximately value-balanced at spot: report an estimate, not an exact TVL.
  p.liquiditySol = round(quoteSol * 2, 4);
  p.liquidity = p.liquiditySol;
  p.liquidityVerified = quoteSol >= CONFIG.minLiquiditySol;
  p.liquidityCheckedAt = new Date().toISOString();
  p.baseReserve = round(baseAmount, 4);
  return {
    ok: p.liquidityVerified,
    reason: p.liquidityVerified ? null : "Liquidité de cotation inférieure à " + CONFIG.minLiquiditySol + " SOL"
  };
}

async function testRoundTrip(inputLamports, mint) {
  const buy = await jupiterOrder(SOL_MINT, mint, inputLamports);
  const tokenOut = String(buy.outAmount);
  const sell = await jupiterOrder(mint, SOL_MINT, tokenOut);
  const solIn = Number(inputLamports);
  const solBack = Number(sell.outAmount);
  const loss = solIn > 0 ? (1 - solBack / solIn) * 100 : null;
  if (!Number.isFinite(loss) || solBack <= 0) throw new Error("Retour SOL invalide");
  return {
    buyOk: true, sellOk: true,
    inputSol: solIn / 1e9, tokenOut, solBack: solBack / 1e9,
    roundTripLossPct: round(loss, 2),
    buyPriceImpactPct: buy.priceImpactPct == null ? null : toNum(buy.priceImpactPct),
    sellPriceImpactPct: sell.priceImpactPct == null ? null : toNum(sell.priceImpactPct)
  };
}

async function checkCandidate(p) {
  if (!p || !state.running) return;
  p.lastCheckAt = now();
  p.checkError = null;
  const risks = [];
  try {
    p.security = await inspectMint(p.mint);
    risks.push(...p.security.risks);
  } catch (e) {
    p.security = { checked: false, error: safeError(e), risks: [] };
    risks.push("Sécurité mint non vérifiée");
  }

  if (JUPITER_API_KEY) {
    try {
      const solPrice = await refreshSolPrice();
      for (const swap of p.swaps) {
        swap.solEquivalent = swap.usdAmount != null ? swap.usdAmount / solPrice : swap.solAmount;
      }
      recalculateSwapMetrics(p);
    } catch (_) {
      // Keep stablecoin-denominated flow unverified if the SOL/USD reference is unavailable.
    }
  }

  try {
    const market = await verifyPoolLiquidity(p);
    if (!market.ok) risks.push(market.reason);
  } catch (e) {
    p.liquidityVerified = false;
    p.liquidity = null;
    p.liquiditySol = null;
    p.quoteReserveSol = null;
    risks.push("Liquidité du pool non vérifiée : " + safeError(e));
  }

  let small = null, medium = null, roundTripLossPct = null;
  if (p.security.checked && p.security.risks.length === 0 && JUPITER_API_KEY) {
    try {
      state.jupiter.lastCheck = new Date().toISOString();
      small = await testRoundTrip(CONFIG.quoteLadderLamports[0], p.mint);
      medium = await testRoundTrip(CONFIG.quoteLadderLamports[1], p.mint);
      roundTripLossPct = small.roundTripLossPct;
      p.quotes = {
        buyOk: small.buyOk && medium.buyOk,
        sellOk: small.sellOk && medium.sellOk,
        testInputSol: small.inputSol,
        tokenOut: small.tokenOut,
        solBack: small.solBack,
        roundTripLossPct: small.roundTripLossPct,
        buyPriceImpactPct: small.buyPriceImpactPct,
        sellPriceImpactPct: small.sellPriceImpactPct,
        quoteLadder: [small, medium],
        checkedAt: new Date().toISOString()
      };
      state.jupiter.lastError = null;
      if (small.roundTripLossPct > CONFIG.maxRoundTripLossPct || medium.roundTripLossPct > CONFIG.maxRoundTripLossPct) {
        risks.push("Coût aller-retour supérieur à " + CONFIG.maxRoundTripLossPct + "% sur la grille de cotations");
      }
    } catch (e) {
      p.quotes = { buyOk: Boolean(small?.buyOk || medium?.buyOk), sellOk: Boolean(small?.sellOk || medium?.sellOk), quoteLadder: [small, medium].filter(Boolean), error: safeError(e), checkedAt: new Date().toISOString() };
      state.jupiter.lastError = safeError(e);
      risks.push("Grille de cotations achat/revente incomplète");
    }
  } else if (!JUPITER_API_KEY) {
    risks.push("Clé Jupiter absente : test achat/revente impossible");
  } else {
    risks.push("Sécurité mint non validée");
  }

  const total = p.buyCount + p.sellCount;
  const buyers = p.buyers.size;
  const pressure = total ? p.buyCount / total : 0;
  p.buyPressure = pressure;
  if (total < CONFIG.minSwaps) risks.push("Pas assez de swaps observés (" + total + "/" + CONFIG.minSwaps + ")");
  if (buyers < CONFIG.minBuyers) risks.push("Pas assez d'acheteurs distincts (" + buyers + "/" + CONFIG.minBuyers + ")");
  if (pressure < CONFIG.minBuyPressure) risks.push("Pression acheteuse insuffisante en nombre de swaps");
  if (p.volumeBuyPressure == null) risks.push("Pression acheteuse en volume non vérifiable");
  else if (p.volumeBuyPressure < CONFIG.minBuyPressure) risks.push("Pression acheteuse insuffisante en volume");

  const securityScore = p.security.checked && p.security.risks.length === 0 ? 25 : 0;
  const activityScore = Math.min(15, total * 1.5);
  const buyerScore = Math.min(15, buyers * 3);
  const pressureScore = Math.round(15 * pressure);
  const volumeScore = p.volumeBuyPressure == null ? 0 : Math.round(15 * p.volumeBuyPressure);
  const liquidityScore = p.liquidityVerified ? 10 : 0;
  const quoteScore = p.quotes.buyOk && p.quotes.sellOk && roundTripLossPct != null && roundTripLossPct <= CONFIG.maxRoundTripLossPct ? 5 : 0;
  p.score = Math.round(securityScore + activityScore + buyerScore + pressureScore + volumeScore + liquidityScore + quoteScore);
  p.eligible = risks.length === 0 && Boolean(p.poolAddress) && p.liquidityVerified &&
    p.quotes.buyOk && p.quotes.sellOk && small && medium &&
    small.roundTripLossPct <= CONFIG.maxRoundTripLossPct &&
    medium.roundTripLossPct <= CONFIG.maxRoundTripLossPct;
  p.risks = [...new Set(risks)];
  p.roundTripLossPct = roundTripLossPct == null ? null : round(roundTripLossPct, 2);
  p.buyersCount = buyers;
  p.activityCount = total;
  p.checkedAt = new Date().toISOString();
}

async function runChecks() {
  if (!state.running || state.scanning) return;
  state.scanning = true;
  try {
    const candidates = [...state.pools.values()]
      .filter(p => now() - p.lastCheckAt >= CONFIG.checkEveryMs)
      .sort((a, b) => (b.swaps.length - a.swaps.length) || (b.lastSwapAt - a.lastSwapAt))
      .slice(0, CONFIG.checkBatch);
    for (const p of candidates) {
      if (!state.running) break;
      await checkCandidate(p);
      await sleep(250);
    }
    state.lastUpdate = new Date().toISOString();
  } catch (e) {
    state.lastError = safeError(e);
  } finally {
    state.scanning = false;
  }
}

function rememberSignature(signature) {
  if (!signature || state.seenSignatures.has(signature)) return false;
  state.seenSignatures.add(signature);
  if (state.seenSignatures.size > CONFIG.maxSeenSignatures) {
    const first = state.seenSignatures.values().next().value;
    state.seenSignatures.delete(first);
  }
  return true;
}

async function processSignature(signature, dexName, slot) {
  try {
    const tx = await heliusRpc("getTransaction", [signature, {
      encoding: "jsonParsed",
      commitment: "confirmed",
      maxSupportedTransactionVersion: 1
    }]);
    state.helius.lastCheck = new Date().toISOString();
    state.helius.lastSlot = slot || tx?.slot || state.helius.lastSlot;
    if (!tx || !tx.meta || tx.meta.err) return;
    const swap = findSwapForSigner(tx);
    if (!swap || !swap.mint || swap.mint === SOL_MINT || swap.mint === USDC_MINT || swap.mint === USDT_MINT) return;
    const market = extractRaydiumCpmmMarket(tx, swap.mint);
    addSwap(swap.mint, swap, dexName, signature, slot || tx.slot, market);
    const p = state.pools.get(swap.mint);
    if (p && !p.metadataRequested) {
      p.metadataRequested = true;
      enrichCandidate(p).catch(() => {});
    }
    state.discovery.successCount++;
    state.discovery.lastCheck = new Date().toISOString();
    state.scanner = "LIVE";
  } catch (e) {
    state.helius.lastError = safeError(e);
    state.discovery.failureCount++;
    state.discovery.lastError = safeError(e);
  }
}

async function drainQueue() {
  while (state.running && state.processing < CONFIG.maxConcurrentTransactions && state.signatureQueue.length) {
    const item = state.signatureQueue.shift();
    state.processing++;
    processSignature(item.signature, item.dexName, item.slot)
      .catch(e => { state.helius.lastError = safeError(e); })
      .finally(() => {
        state.processing--;
        drainQueue();
      });
  }
}

function startHeliusSocket() {
  if (!state.running || !HELIUS_RPC_URL) {
    state.scanner = HELIUS_RPC_URL ? "STOPPED" : "ERROR";
    if (!HELIUS_RPC_URL) state.lastError = "HELIUS_RPC_URL non configurée";
    return;
  }
  const WebSocketImpl = globalThis.WebSocket;
  if (!WebSocketImpl) {
    state.scanner = "ERROR";
    state.lastError = "WebSocket natif absent : configure Node.js 22 ou supérieur sur Render";
    return;
  }
  const url = rpcWebSocketUrl();
  if (!url) {
    state.scanner = "ERROR";
    state.lastError = "URL WebSocket Helius invalide";
    return;
  }
  try {
    state.scanner = "CONNECTING";
    const ws = new WebSocketImpl(url);
    state.ws = ws;
    let requestId = 1;
    const pending = new Map();
    const subscriptions = new Map();

    ws.addEventListener("open", () => {
      state.helius.connected = true;
      state.helius.lastError = null;
      state.scanner = "LIVE";
      for (const program of DEX_PROGRAMS) {
        const id = requestId++;
        pending.set(id, program);
        ws.send(JSON.stringify({
          jsonrpc: "2.0",
          id,
          method: "logsSubscribe",
          params: [{ mentions: [program.id] }, { commitment: "confirmed" }]
        }));
      }
      state.helius.subscriptions = DEX_PROGRAMS.length;
      state.discovery.callsCount += DEX_PROGRAMS.length;
      state.discovery.lastCheck = new Date().toISOString();
    });

    ws.addEventListener("message", event => {
      let msg;
      try { msg = JSON.parse(String(event.data)); } catch (_) { return; }
      if (msg.id && pending.has(msg.id)) {
        const program = pending.get(msg.id);
        pending.delete(msg.id);
        if (msg.error) {
          state.helius.lastError = msg.error.message || "Échec abonnement Helius";
          state.discovery.failureCount++;
        } else {
          subscriptions.set(msg.result, program);
          state.helius.subscriptions = subscriptions.size;
        }
        return;
      }
      if (msg.method !== "logsNotification") return;
      const result = msg.params?.result;
      const value = result?.value;
      const signature = value?.signature;
      if (!signature || value.err) return;
      const logs = Array.isArray(value.logs) ? value.logs : [];
      // Keep only swap-like transactions; pool setup/admin traffic is not a trading signal.
      // Accept explicit swap instruction logs, not any incidental mention of the word "swap".
      if (!logs.some(line => /Program log: Instruction:.*\bswap[a-z0-9_]*\b/i.test(String(line)))) return;
      if (!rememberSignature(signature)) return;
      const subId = result?.subscription;
      const program = subscriptions.get(subId);
      state.helius.transactionsSeen++;
      // Keep the newest events if the RPC backlog grows beyond a safe bound.
      if (state.signatureQueue.length >= CONFIG.maxQueuedSignatures) {
        state.signatureQueue.shift();
        state.helius.droppedQueuedTransactions++;
      }
      state.signatureQueue.push({ signature, dexName: program?.name || "DEX Solana", slot: result?.context?.slot || null });
      drainQueue();
    });

    ws.addEventListener("error", () => {
      state.helius.lastError = "Erreur WebSocket Helius";
      state.scanner = "DEGRADED";
    });

    ws.addEventListener("close", () => {
      state.helius.connected = false;
      state.helius.subscriptions = 0;
      if (!state.running) return;
      state.scanner = "DEGRADED";
      state.helius.reconnects++;
      clearTimeout(state.wsReconnectTimer);
      state.wsReconnectTimer = setTimeout(startHeliusSocket, Math.min(30000, 3000 * Math.max(1, state.helius.reconnects)));
    });
  } catch (e) {
    state.helius.connected = false;
    state.scanner = "DEGRADED";
    state.helius.lastError = safeError(e);
    clearTimeout(state.wsReconnectTimer);
    state.wsReconnectTimer = setTimeout(startHeliusSocket, 10000);
  }
}

function publicCandidate(p) {
  const swaps = p.swaps || [];
  return {
    address: p.mint,
    mint: p.mint,
    poolAddress: p.poolAddress,
    name: p.name,
    symbol: p.symbol,
    dex: p.dex,
    liquidity: p.liquidity,
    liquidityVerified: Boolean(p.liquidityVerified),
    liquiditySol: p.liquiditySol,
    estimatedLiquiditySol: p.liquiditySol,
    quoteReserveSol: p.quoteReserveSol,
    quoteLiquidity: p.quoteLiquidity,
    quoteMint: p.quoteMint,
    volume: p.volume,
    volumeUsd: p.volumeUsd,
    volumeSol: round(p.volumeSol, 6),
    price: null,
    created: p.created,
    firstSeenAt: new Date(p.firstSeenAt).toISOString(),
    lastSwapAt: p.lastSwapAt ? new Date(p.lastSwapAt).toISOString() : null,
    activityWindowMinutes: 15,
    activityCount: swaps.length,
    buyCount: p.buyCount,
    sellCount: p.sellCount,
    buyersCount: p.buyers.size,
    buyPressure: round(p.buyPressure * 100, 1),
    buyPressureByVolume: p.volumeBuyPressure == null ? null : round(p.volumeBuyPressure * 100, 1),
    buyVolumeSol: round(p.buyVolumeSol, 6),
    sellVolumeSol: round(p.sellVolumeSol, 6),
    score: p.score,
    eligible: p.eligible,
    risks: p.risks,
    security: p.security,
    quotes: p.quotes,
    roundTripLossPct: p.roundTripLossPct,
    checkedAt: p.checkedAt || null
  };
}

function statusPayload() {
  const pools = [...state.pools.values()];
  const eligible = pools.filter(p => p.eligible);
  return {
    running: state.running,
    mode: "TEST",
    realTrading: false,
    scanner: state.scanner,
    poolsFound: pools.length,
    catalogPools: pools.length,
    eligiblePools: eligible.length,
    lastUpdate: state.lastUpdate,
    lastError: state.lastError,
    scanning: state.scanning,
    nextScanInSeconds: state.running ? Math.max(0, Math.ceil(CONFIG.checkEveryMs / 1000)) : 0,
    startedAt: state.startedAt,
    discovery: { ...state.discovery, cachedFeeds: pools.length },
    helius: { ...state.helius, configured: Boolean(HELIUS_RPC_URL) },
    jupiter: {
      ...state.jupiter,
      configured: Boolean(JUPITER_API_KEY),
      cooldownSeconds: Math.max(0, Math.ceil((jupiterCooldownUntil - Date.now()) / 1000)),
      minIntervalMs: JUPITER_MIN_INTERVAL_MS,
      queueDepth: jupiterQueueDepth
    },
    protections: { ...state.protections }
  };
}

app.get("/", (_req, res) => res.json({
  name: "CART3R.JAILBREAK",
  version: "HELIUS-ONCHAIN-1",
  mode: "TEST",
  realTrading: false,
  message: "Scanner Helius on-chain actif. Aucun ordre réel n'est exécuté.",
  endpoints: ["/health", "/status", "/pools", "/security", "/helius-test", "/jupiter-test", "/gecko-test", "/test/start", "/test/stop"]
}));

app.get("/health", (_req, res) => res.json({ ok: true, service: "CART3R", mode: "TEST", realTrading: false }));
app.get("/status", (_req, res) => res.json(statusPayload()));
app.get("/pools", (_req, res) => {
  const list = [...state.pools.values()].map(publicCandidate)
    .sort((a, b) => Number(b.eligible) - Number(a.eligible) || b.score - a.score || b.activityCount - a.activityCount)
    .slice(0, CONFIG.maxCandidates);
  res.json({ ok: true, mode: "TEST", realTrading: false, count: list.length, pools: list });
});
app.get("/security", async (req, res) => {
  const mint = String(req.query.address || req.query.mint || "").trim();
  if (!mint) return res.status(400).json({ ok: false, error: "Paramètre address ou mint manquant" });
  try {
    const security = await inspectMint(mint);
    res.json({ ok: true, mint, security, safe: security.risks.length === 0, realTrading: false });
  } catch (e) {
    res.status(502).json({ ok: false, mint, error: safeError(e), safe: false });
  }
});
app.get("/helius-test", async (_req, res) => {
  try {
    const slot = await heliusRpc("getSlot", [{ commitment: "confirmed" }]);
    state.helius.lastCheck = new Date().toISOString();
    state.helius.lastSlot = slot;
    state.helius.lastError = null;
    res.json({ ok: true, slot, checkedAt: state.helius.lastCheck, websocketConnected: state.helius.connected });
  } catch (e) {
    state.helius.lastError = safeError(e);
    res.status(502).json({ ok: false, error: safeError(e) });
  }
});
app.get("/jupiter-test", async (_req, res) => {
  try {
    const quote = await jupiterOrder(SOL_MINT, USDC_MINT, "1000000000");
    state.jupiter.lastCheck = new Date().toISOString();
    state.jupiter.lastSolPriceUsd = Number(quote.outAmount) / 1e6;
    state.jupiter.lastSolPriceAt = now();
    state.jupiter.lastError = null;
    res.json({ ok: true, api: "SWAP_V2_ORDER", inputSol: 1, outputUsdc: state.jupiter.lastSolPriceUsd, checkedAt: state.jupiter.lastCheck, quoteOnly: true });
  } catch (e) {
    state.jupiter.lastError = safeError(e);
    res.status(502).json({ ok: false, error: safeError(e), quoteOnly: true });
  }
});
app.get("/gecko-test", (_req, res) => res.json({
  ok: true,
  disabled: true,
  provider: "HELIUS",
  message: "GeckoTerminal a été retiré du scanner pour éviter les erreurs HTTP 429."
}));
app.post("/test/start", (_req, res) => {
  if (!state.running) {
    state.running = true;
    state.scanner = "STARTING";
    startHeliusSocket();
  }
  res.json({ ok: true, running: state.running, mode: "TEST", realTrading: false });
});
app.post("/test/stop", (_req, res) => {
  state.running = false;
  state.scanner = "STOPPED";
  clearTimeout(state.wsReconnectTimer);
  clearInterval(state.checkTimer);
  if (state.ws) {
    try { state.ws.close(); } catch (_) {}
    state.ws = null;
  }
  state.helius.connected = false;
  res.json({ ok: true, running: false, mode: "TEST", realTrading: false });
});

const server = app.listen(PORT, () => {
  console.log("CART3R Helius scanner listening on", PORT, "mode=TEST realTrading=false");
  if (!HELIUS_RPC_URL) {
    state.scanner = "ERROR";
    state.lastError = "HELIUS_RPC_URL non configurée";
  } else {
    startHeliusSocket();
  }
  state.checkTimer = setInterval(runChecks, 15000);
});

process.on("SIGTERM", () => {
  state.running = false;
  clearTimeout(state.wsReconnectTimer);
  clearInterval(state.checkTimer);
  if (state.ws) { try { state.ws.close(); } catch (_) {} }
  server.close(() => process.exit(0));
});
