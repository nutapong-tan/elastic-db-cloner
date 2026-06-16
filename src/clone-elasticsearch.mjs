import { Client } from "@elastic/elasticsearch";
import dotenv from "dotenv";

dotenv.config({ path: process.env.ENV_FILE || ".env" });

// Runtime options. The .env file only needs credentials; everything else has a safe default.
const args = new Set(process.argv.slice(2));
const verifyRead = boolEnv("VERIFY_READ", false) || args.has("--verify-read") || args.has("--deep-check");

const config = {
  dryRun: boolEnv("DRY_RUN", false) || args.has("--dry-run") || verifyRead,
  verifyRead,
  sourceOnly: args.has("--source-only"),
  confirmed: process.env.CONFIRM_CLONE === "yes" || args.has("--yes"),
  overwriteTargetIndices: boolEnv("OVERWRITE_TARGET_INDICES", false) || args.has("--overwrite"),
  includeSystemIndices: boolEnv("INCLUDE_SYSTEM_INDICES", false) || args.has("--include-system"),
  createAliases: boolEnv("CREATE_ALIASES", true),
  copyLifecycleSettings: boolEnv("COPY_LIFECYCLE_SETTINGS", false),
  batchSize: numberEnv("BATCH_SIZE", 1000),
  scrollTtl: process.env.SCROLL_TTL || "5m",
  requestTimeout: numberEnv("REQUEST_TIMEOUT_MS", 300000),
  maxRetries: numberEnv("MAX_RETRIES", 3),
  targetIndexPrefix: process.env.TARGET_INDEX_PREFIX || "",
  targetIndexSuffix: process.env.TARGET_INDEX_SUFFIX || "",
  targetAliasPrefix: process.env.TARGET_ALIAS_PREFIX || "",
  targetAliasSuffix: process.env.TARGET_ALIAS_SUFFIX || "",
  targetNumberOfReplicas: optionalNumberString("TARGET_NUMBER_OF_REPLICAS"),
  allowList: parseCsv(process.env.INDEX_ALLOWLIST || process.env.INDEX_PATTERNS || "*"),
  excludeList: parseCsv(process.env.INDEX_EXCLUDELIST || process.env.INDEX_EXCLUDE || "")
};

main().catch((error) => {
  console.error("");
  console.error("Clone failed.");
  console.error(formatError(error));
  process.exitCode = 1;
});

// Keep the high-level clone flow in one place so the script reads step by step.
async function main() {
  const writeMode = !config.dryRun;
  const checkTarget = !config.sourceOnly;

  console.log("Step 1/4: Validate run mode");
  if (config.sourceOnly && !config.dryRun) {
    throw new Error("--source-only can only be used with dry run.");
  }

  if (writeMode && !config.confirmed) {
    throw new Error("Refusing to write to target cluster. Set CONFIRM_CLONE=yes or run with --yes.");
  }

  console.log("Step 2/4: Connect to Elasticsearch");
  const source = makeClient("SOURCE");
  const target = checkTarget ? makeClient("TARGET") : undefined;

  const [sourceInfo, targetInfo] = await Promise.all([
    clientInfo(source),
    target ? clientInfo(target) : undefined
  ]);

  console.log(`Source: ${sourceInfo.cluster_name || "unknown"} (${sourceInfo.version?.number || "unknown"})`);
  if (targetInfo) {
    console.log(`Target: ${targetInfo.cluster_name || "unknown"} (${targetInfo.version?.number || "unknown"})`);
  } else {
    console.log("Target: skipped (--source-only)");
  }

  const sameCluster = sourceInfo.cluster_uuid && targetInfo?.cluster_uuid && sourceInfo.cluster_uuid === targetInfo.cluster_uuid;
  const renamingIndices = Boolean(config.targetIndexPrefix || config.targetIndexSuffix);
  if (writeMode && sameCluster && !renamingIndices) {
    throw new Error("Source and target look like the same cluster. Set TARGET_INDEX_PREFIX or TARGET_INDEX_SUFFIX to clone safely.");
  }

  console.log("");
  console.log("Step 3/4: Select source indices");
  const sourceIndices = await listSourceIndices(source);
  const selectedIndices = sourceIndices.filter((row) => shouldCloneIndex(row.index));

  if (selectedIndices.length === 0) {
    console.log("No indices matched the current filters.");
    return;
  }

  console.log("");
  console.log(`Selected ${selectedIndices.length} index(es):`);
  for (const row of selectedIndices) {
    const docs = row["docs.count"] ?? "?";
    const storage = row["store.size"] ?? "?";
    console.log(`- ${row.index} (${docs} docs, ${storage})`);
  }

  console.log("");
  console.log(`Step 4/4: ${writeMode ? "Clone selected indices" : "Run read-only check"}`);
  if (!writeMode) {
    if (config.verifyRead) {
      await verifySelectedIndices(source, target, selectedIndices);
    } else {
      console.log("Dry run only. No target data was changed.");
      console.log("Tip: run with --verify-read to also read settings, mappings, aliases, and documents.");
    }
    return;
  }

  const summary = [];
  for (const row of selectedIndices) {
    const sourceIndex = row.index;
    const targetIndex = transformIndexName(sourceIndex);

    console.log("");
    console.log(`==> ${sourceIndex} -> ${targetIndex}`);

    const metadata = await loadIndexMetadata(source, sourceIndex, targetIndex);
    await createTargetIndex(target, metadata);

    const copied = await copyDocuments(source, target, sourceIndex, targetIndex);
    const sourceCount = await countDocuments(source, sourceIndex);
    await target.indices.refresh({ index: targetIndex });
    const targetCount = await countDocuments(target, targetIndex);

    if (sourceCount !== targetCount) {
      throw new Error(`Count mismatch for ${sourceIndex}: source=${sourceCount}, target=${targetCount}`);
    }

    summary.push({ sourceIndex, targetIndex, copied, sourceCount, targetCount });
    console.log(`Verified ${targetIndex}: ${targetCount} docs`);
  }

  console.log("");
  console.log("Clone complete.");
  for (const item of summary) {
    console.log(`- ${item.sourceIndex} -> ${item.targetIndex}: ${item.targetCount} docs`);
  }
}

// Read-only mode uses the same metadata/document readers, but never writes to target.
async function verifySelectedIndices(source, target, selectedIndices) {
  console.log("Read-only verification. No target data will be changed.");

  const summary = [];

  for (const row of selectedIndices) {
    const sourceIndex = row.index;
    const targetIndex = transformIndexName(sourceIndex);

    console.log("");
    console.log(`==> verifying ${sourceIndex} -> ${targetIndex}`);

    const [metadata, sourceCount, targetExists] = await Promise.all([
      loadIndexMetadata(source, sourceIndex, targetIndex),
      countDocuments(source, sourceIndex),
      target ? target.indices.exists({ index: targetIndex }).then(unwrap) : undefined
    ]);

    const readResult = await verifyReadableDocuments(source, sourceIndex, targetIndex);

    if (sourceCount !== readResult.read) {
      throw new Error(`Read count mismatch for ${sourceIndex}: count API=${sourceCount}, scroll read=${readResult.read}`);
    }

    const item = {
      sourceIndex,
      targetIndex,
      sourceCount,
      targetExists,
      batches: readResult.batches,
      routed: readResult.routed,
      settings: Object.keys(metadata.settings).length,
      aliases: Object.keys(metadata.aliases).length,
      fields: countMappingFields(metadata.mappings)
    };
    summary.push(item);

    console.log(`Metadata readable: ${item.settings} setting key(s), ${item.fields} mapped field(s), ${item.aliases} alias(es)`);
    console.log(`Documents readable: ${readResult.read} doc(s) in ${readResult.batches} batch(es), ${readResult.routed} with routing`);
    if (target) {
      console.log(`Target index currently ${targetExists ? "exists" : "does not exist"}: ${targetIndex}`);
    } else {
      console.log(`Target index check skipped: ${targetIndex}`);
    }
  }

  console.log("");
  console.log("Dry run read verification complete. No target data was changed.");
  for (const item of summary) {
    const targetStatus = item.targetExists === undefined ? "skipped" : item.targetExists ? "exists" : "missing";
    console.log(`- ${item.sourceIndex} -> ${item.targetIndex}: ${item.sourceCount} doc(s), ${item.fields} mapped field(s), target ${targetStatus}`);
  }
}

// Elasticsearch client and index metadata helpers.
function makeClient(prefix) {
  const cloudId = process.env[`${prefix}_ELASTICSEARCH_CLOUD_ID`];
  const node = process.env[`${prefix}_ELASTICSEARCH_HOST`];
  const apiKey = process.env[`${prefix}_ELASTICSEARCH_API_KEY`];
  const username = process.env[`${prefix}_ELASTICSEARCH_USERNAME`];
  const password = process.env[`${prefix}_ELASTICSEARCH_PASSWORD`];
  const rejectUnauthorized = boolEnv(`${prefix}_ELASTICSEARCH_TLS_REJECT_UNAUTHORIZED`, true);

  if (!cloudId && !node) {
    throw new Error(`${prefix}_ELASTICSEARCH_CLOUD_ID or ${prefix}_ELASTICSEARCH_HOST is required.`);
  }

  const clientConfig = {
    requestTimeout: config.requestTimeout,
    maxRetries: config.maxRetries,
    tls: { rejectUnauthorized }
  };

  if (cloudId) {
    if (!cloudId.includes(":")) {
      throw new Error(`${prefix}_ELASTICSEARCH_CLOUD_ID must use Elastic Cloud ID format "name:base64string". If you have an HTTPS endpoint, set ${prefix}_ELASTICSEARCH_HOST instead.`);
    }

    clientConfig.cloud = { id: cloudId };
  } else {
    clientConfig.node = node;
  }

  if (apiKey) {
    clientConfig.auth = { apiKey };
  } else if (username || password) {
    if (!username || !password) {
      throw new Error(`${prefix}_ELASTICSEARCH_USERNAME and ${prefix}_ELASTICSEARCH_PASSWORD must be set together.`);
    }
    clientConfig.auth = { username, password };
  }

  return new Client(clientConfig);
}

async function clientInfo(client) {
  return unwrap(await client.info());
}

async function listSourceIndices(client) {
  const expandWildcards = config.includeSystemIndices ? "open,hidden" : "open";
  const rows = unwrap(await client.cat.indices({
    format: "json",
    h: "index,docs.count,store.size,health,status",
    expand_wildcards: expandWildcards
  }));

  return rows
    .filter((row) => row.index && row.status !== "close")
    .sort((a, b) => a.index.localeCompare(b.index));
}

function shouldCloneIndex(indexName) {
  if (!config.includeSystemIndices && indexName.startsWith(".")) {
    return false;
  }

  if (!matchesAny(indexName, config.allowList)) {
    return false;
  }

  if (config.excludeList.length > 0 && matchesAny(indexName, config.excludeList)) {
    return false;
  }

  return true;
}

async function loadIndexMetadata(source, sourceIndex, targetIndex) {
  const expandWildcards = config.includeSystemIndices ? "open,hidden" : "open";
  const [settingsResponse, mappingsResponse, aliasesResponse] = await Promise.all([
    source.indices.getSettings({ index: sourceIndex, expand_wildcards: expandWildcards }),
    source.indices.getMapping({ index: sourceIndex, expand_wildcards: expandWildcards }),
    getAliases(source, sourceIndex, expandWildcards)
  ]);

  const settingsBody = unwrap(settingsResponse);
  const mappingsBody = unwrap(mappingsResponse);
  const aliasesBody = unwrap(aliasesResponse);

  const sourceSettings = settingsBody[sourceIndex]?.settings?.index || {};
  const mappings = mappingsBody[sourceIndex]?.mappings || {};
  const aliases = aliasesBody[sourceIndex]?.aliases || {};

  return {
    sourceIndex,
    targetIndex,
    settings: sanitizeIndexSettings(sourceSettings),
    mappings,
    aliases: transformAliases(aliases)
  };
}

async function getAliases(source, sourceIndex, expandWildcards) {
  if (!config.createAliases) {
    return { [sourceIndex]: { aliases: {} } };
  }

  try {
    return await source.indices.getAlias({ index: sourceIndex, expand_wildcards: expandWildcards });
  } catch (error) {
    if (statusCode(error) === 404) {
      return { [sourceIndex]: { aliases: {} } };
    }
    throw error;
  }
}

async function createTargetIndex(target, metadata) {
  const exists = unwrap(await target.indices.exists({ index: metadata.targetIndex }));

  if (exists) {
    if (!config.overwriteTargetIndices) {
      throw new Error(`Target index already exists: ${metadata.targetIndex}. Set OVERWRITE_TARGET_INDICES=true to recreate it.`);
    }

    console.log(`Deleting existing target index ${metadata.targetIndex}...`);
    await target.indices.delete({ index: metadata.targetIndex });
  }

  const createParams = {
    index: metadata.targetIndex,
    settings: metadata.settings,
    mappings: metadata.mappings
  };

  if (Object.keys(metadata.aliases).length > 0) {
    createParams.aliases = metadata.aliases;
  }

  await target.indices.create(createParams);
  console.log("Created target index with settings and mappings");
}

// Document copy/check helpers.
async function copyDocuments(source, target, sourceIndex, targetIndex) {
  let copied = 0;
  let scrollId;

  try {
    let response = unwrap(await source.search({
      index: sourceIndex,
      scroll: config.scrollTtl,
      size: config.batchSize,
      sort: ["_doc"],
      query: { match_all: {} },
      _source: true
    }));

    scrollId = response._scroll_id;

    while (true) {
      const hits = response.hits?.hits || [];
      if (hits.length === 0) {
        break;
      }

      await bulkIndex(target, targetIndex, hits);
      copied += hits.length;
      console.log(`Copied ${copied} documents...`);

      response = unwrap(await source.scroll({
        scroll_id: scrollId,
        scroll: config.scrollTtl
      }));
      scrollId = response._scroll_id;
    }

    return copied;
  } finally {
    if (scrollId) {
      await source.clearScroll({ scroll_id: scrollId }).catch(() => undefined);
    }
  }
}

async function verifyReadableDocuments(source, sourceIndex, targetIndex) {
  let read = 0;
  let batches = 0;
  let routed = 0;
  let scrollId;

  try {
    let response = unwrap(await source.search({
      index: sourceIndex,
      scroll: config.scrollTtl,
      size: config.batchSize,
      sort: ["_doc"],
      query: { match_all: {} },
      _source: true
    }));

    scrollId = response._scroll_id;

    while (true) {
      const hits = response.hits?.hits || [];
      if (hits.length === 0) {
        break;
      }

      buildBulkOperations(targetIndex, hits);
      read += hits.length;
      batches += 1;
      routed += hits.filter((hit) => hit._routing).length;
      console.log(`Read ${read} documents...`);

      response = unwrap(await source.scroll({
        scroll_id: scrollId,
        scroll: config.scrollTtl
      }));
      scrollId = response._scroll_id;
    }

    return { read, batches, routed };
  } finally {
    if (scrollId) {
      await source.clearScroll({ scroll_id: scrollId }).catch(() => undefined);
    }
  }
}

function buildBulkOperations(targetIndex, hits) {
  const operations = [];

  for (const hit of hits) {
    if (hit._source === undefined || hit._source === null) {
      throw new Error(`Cannot clone ${hit._index}/${hit._id} because _source is disabled or unavailable.`);
    }

    const action = { index: { _index: targetIndex, _id: hit._id } };
    if (hit._routing) {
      action.index.routing = hit._routing;
    }

    operations.push(action, hit._source);
  }

  return operations;
}

async function bulkIndex(target, targetIndex, hits) {
  const operations = buildBulkOperations(targetIndex, hits);

  if (operations.length === 0) {
    return;
  }

  const response = unwrap(await target.bulk({ refresh: false, operations }));
  if (response.errors) {
    const details = response.items
      .map((item) => item.index || item.create || item.update || item.delete)
      .filter((item) => item?.error)
      .slice(0, 5)
      .map((item) => JSON.stringify(item.error))
      .join("\n");
    throw new Error(`Bulk indexing failed for ${targetIndex}.\n${details}`);
  }
}

async function countDocuments(client, index) {
  const response = unwrap(await client.count({ index, query: { match_all: {} } }));
  return response.count;
}

// Small data-shaping helpers.
function sanitizeIndexSettings(sourceSettings) {
  const settings = structuredCloneCompat(sourceSettings);

  delete settings.uuid;
  delete settings.version;
  delete settings.provided_name;
  delete settings.creation_date;
  delete settings.creation_date_string;
  delete settings.resize;
  delete settings.history;
  delete settings.verified_before_close;
  delete settings.blocks;
  delete settings.routing;

  if (!config.copyLifecycleSettings) {
    delete settings.lifecycle;
  }

  if (config.targetNumberOfReplicas !== undefined) {
    settings.number_of_replicas = config.targetNumberOfReplicas;
  }

  return settings;
}

function transformIndexName(indexName) {
  return `${config.targetIndexPrefix}${indexName}${config.targetIndexSuffix}`;
}

function transformAliases(aliases) {
  if (!config.createAliases) {
    return {};
  }

  return Object.fromEntries(
    Object.entries(aliases).map(([name, value]) => [
      `${config.targetAliasPrefix}${name}${config.targetAliasSuffix}`,
      value
    ])
  );
}

function countMappingFields(mappings) {
  return countProperties(mappings?.properties || {});
}

function countProperties(properties) {
  return Object.values(properties).reduce((total, property) => {
    const nested = property?.properties ? countProperties(property.properties) : 0;
    const fields = property?.fields ? Object.keys(property.fields).length : 0;
    return total + 1 + nested + fields;
  }, 0);
}

function matchesAny(value, patterns) {
  return patterns.some((pattern) => wildcardToRegExp(pattern).test(value));
}

function wildcardToRegExp(pattern) {
  const escaped = pattern.replace(/[|\\{}()[\]^$+?.]/g, "\\$&").replace(/\*/g, ".*");
  return new RegExp(`^${escaped}$`);
}

function parseCsv(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

// Environment parsing and error formatting.
function boolEnv(name, defaultValue) {
  const value = process.env[name];
  if (value === undefined || value === "") {
    return defaultValue;
  }

  return ["1", "true", "yes", "on"].includes(value.toLowerCase());
}

function numberEnv(name, defaultValue) {
  const value = process.env[name];
  if (value === undefined || value === "") {
    return defaultValue;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive number.`);
  }

  return parsed;
}

function optionalNumberString(name) {
  const value = process.env[name];
  if (value === undefined || value === "") {
    return undefined;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer.`);
  }

  return String(parsed);
}

function unwrap(response) {
  return response?.body ?? response;
}

function statusCode(error) {
  return error?.meta?.statusCode || error?.statusCode;
}

function formatError(error) {
  if (error?.meta?.body?.error) {
    return JSON.stringify(error.meta.body.error, null, 2);
  }

  return error?.stack || String(error);
}

function structuredCloneCompat(value) {
  return JSON.parse(JSON.stringify(value || {}));
}
