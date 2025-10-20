async function(properties, context) {
  const axios = require('axios');

  // ---- Config / Inputs ----
  const appDomain = context.keys.appID;
  const valueDelimiter = properties.key_delimiter || ",";
  const version   = properties.version || "test";
  const thingType = properties.object_type_text;          // e.g. "order", "product"
  const apiToken  = properties.apiToken || context.keys.appAPIKey;

  // keys_or_raw enum: "Keys" or "JSONL"
  const modeStr   = (properties.keys_or_raw ?? "").toString().toLowerCase();
  const usingKeys = modeStr === "keys";

  // limit (default 25)
  const MAX_COUNT = Number(properties.max_count) > 0 ? Number(properties.max_count) : 25;

  // ---- Helpers ----
  const toStr = v => String(v == null ? "" : v);

  const escapeRegex = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

  // Strip surrounding single/double quotes if present
  const stripOuterQuotes = s => {
    const t = s.trim();
    if ((t.startsWith('"') && t.endsWith('"')) || (t.startsWith("'") && t.endsWith("'"))) {
      return t.slice(1, -1);
    }
    return t;
  };

  // Split a value string into tokens:
  // 1) JSON array -> tokens
  // 2) custom delimiter -> tokens
  // 3) comma fallback -> tokens
  const splitValueList = (raw, delimiter) => {
    let s = toStr(raw).trim();
    if (!s) return [""];

    // JSON array support: ["a","b"] or [1,2]
    if (s.startsWith("[")) {
      try {
        const arr = JSON.parse(s);
        if (Array.isArray(arr)) {
          const out = arr.map(x => stripOuterQuotes(toStr(x)));
          return out.length ? out : [""];
        }
      } catch (_) { /* fall through */ }
    }

    // Custom delimiter (treated literally)
    const del = delimiter || ",";
    const delRegex = new RegExp(escapeRegex(del), "g");
    let parts = s.split(delRegex);
    if (parts.length > 1) {
      parts = parts.map(x => stripOuterQuotes(toStr(x))).map(x => x.trim());
      return parts.filter(x => x.length || x === "") || [""];
    }

    // Comma fallback
    parts = s.split(/\s*,\s*/);
    parts = parts.map(x => stripOuterQuotes(toStr(x))).map(x => x.trim());
    return parts.filter(x => x.length || x === "") || [""];
  };

  // Build JSONL (array of lines) from json_keys = [{ key, value }],
  // where each 'value' is a delimited list; produce one line per index.
  const buildJsonlFromKeyLists = (pairs, delimiter, persistentPairs) => {
    const cols = (Array.isArray(pairs) ? pairs : [])
      .filter(p => p && p.key != null)
      .map(p => {
        const k = toStr(p.key).trim();
        if (!k) return null;
        const values = splitValueList(p.value, delimiter);
        return { key: k, values };
      })
      .filter(Boolean);

    if (!cols.length) return { lines: [], count: 0 };

    // Process persistent key-value pairs that should be added to every row
    const persistentObj = {};
    if (Array.isArray(persistentPairs)) {
      persistentPairs
        .filter(p => p && p.key != null)
        .forEach(p => {
          const k = toStr(p.key).trim();
          if (k) {
            // Use splitValueList to handle user mistakes (quotes, JSON, etc.) and take first value
            const processedValues = splitValueList(p.value, delimiter);
            persistentObj[k] = processedValues[0] || "";
          }
        });
    }

    const rowCount = cols.reduce((m, c) => Math.max(m, c.values.length), 0);

    const lines = [];
    for (let i = 0; i < rowCount; i++) {
      const obj = { ...persistentObj }; // Start with persistent values
      for (const c of cols) {
        const v = (c.values[i] !== undefined ? c.values[i] : "");
        obj[c.key] = toStr(v); // force string; JSON.stringify of whole obj below
      }
      lines.push(JSON.stringify(obj));
    }
    return { lines, count: rowCount };
  };

  // Robustly normalise any NDJSON-ish input into lines
  const toJsonlLines = (input) => {
    if (typeof input === "string") {
      const s = input.trim();
      if (!s) return [];
      if (s.startsWith("[")) {
        try {
          const arr = JSON.parse(s);
          if (Array.isArray(arr)) {
            return arr
              .filter(x => x !== null && x !== undefined)
              .map(x => typeof x === "string" ? x : JSON.stringify(x));
          }
        } catch (_) {}
      }
      return s.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
    }
    if (Array.isArray(input)) {
      return input
        .filter(x => x !== null && x !== undefined && String(x).trim() !== "")
        .map(x => typeof x === "string" ? x.trim() : JSON.stringify(x));
    }
    if (input && typeof input === "object") {
      return [JSON.stringify(input)];
    }
    return [];
  };

  // ---- Construct NDJSON body ----
  let ndjsonBody = "";

  if (usingKeys) {
    // Expect: properties.json_keys = Array<{ key, value }>, where value is a delimited *list* string
    const keyPairs = properties.json_keys || [];
    if (!Array.isArray(keyPairs) || keyPairs.length === 0) {
      return { returned_error: true, error_message: "json_keys must be a non-empty array of {key, value} in Keys mode." };
    }

    // Get persistent key-value pairs that should be added to every row
    const persistentKeyPairs = properties.json_keys_persistent || [];

    const { lines, count } = buildJsonlFromKeyLists(keyPairs, valueDelimiter, persistentKeyPairs);
    if (count === 0) {
      return { returned_error: true, error_message: "No rows could be formed from the provided key/value lists." };
    }
    if (count > MAX_COUNT) {
      return { returned_error: true, error_message: `Too many rows from key lists: ${count}. Max allowed is ${MAX_COUNT}.` };
    }
    ndjsonBody = lines.join("\n");
  } else {
    // JSONL mode — gather lines from various inputs
    let lines = [];

    if (await properties.ndjson_list && typeof properties.ndjson_list.length === "function") {
      const len = await properties.ndjson_list.length();
      const list = len ? await properties.ndjson_list.get(0, len) : [];
      lines = lines.concat(toJsonlLines(list));
    }

    const fromNdjson = (properties.ndjson !== undefined) ? properties.ndjson : null;
    const fromItems  = (properties.items  !== undefined) ? properties.items  : null;

    if (fromNdjson !== null) lines = lines.concat(toJsonlLines(fromNdjson));
    if (fromItems  !== null) lines = lines.concat(toJsonlLines(fromItems));

    lines = lines.map(l => l.trim()).filter(Boolean);
    if (lines.length === 0) {
      return { returned_error: true, error_message: "NDJSON body is empty. Provide 'ndjson' (text/array), 'items' (text/array), or 'ndjson_list' (list of text/objects)." };
    }
    if (lines.length > MAX_COUNT) {
      return { returned_error: true, error_message: `Too many NDJSON rows: ${lines.length}. Max allowed is ${MAX_COUNT}.` };
    }

    ndjsonBody = lines.join("\n");
  }

  // ---- Validate basics ----
  if (!thingType) return { returned_error: true, error_message: "thingType is required." };
  if (!apiToken)  return { returned_error: true, error_message: "Missing Bubble API token." };

  const url = `https://${appDomain}.bubbleapps.io/version-${version}/api/1.1/obj/${thingType}/bulk`;

  const requested_count = ndjsonBody.split(/\r?\n/).filter(l => l && l.trim() !== "").length;

  // ---- API call (accept JSON array or NDJSON response) ----
  const doBulkCreate = () => context.v3.async(async callback => {
    try {
      const res = await axios.post(url, ndjsonBody, {
        headers: {
          "Authorization": `Bearer ${apiToken}`,
          "Content-Type": "text/plain",
          "Accept": "*/*"
        },
        responseType: "text",
        validateStatus: s => s >= 200 && s < 300
      });

      const raw = (res && typeof res.data === "string") ? res.data : "";
      let results = [];

      const trimmed = raw.trim();
      if (trimmed.startsWith("[")) {
        try { results = JSON.parse(trimmed); } catch {}
      }
      if (results.length === 0 && trimmed.length) {
        const lines = trimmed.split("\n").filter(Boolean);
        results = lines.map(l => { try { return JSON.parse(l); } catch { return { status: "error", raw: l }; } });
      }

      callback(null, { raw, results });
    } catch (err) {
      const body = err && err.response && typeof err.response.data === "string"
        ? err.response.data
        : (err.message || String(err));
      callback(new Error(`Bubble bulk create failed: ${body}`));
    }
  });

  return doBulkCreate()
    .then(payload => {
      const results    = Array.isArray(payload.results) ? payload.results : [];
      const successes  = results.filter(r => r && r.status === "success" && r.id);
      const successIds = successes.map(s => String(s.id));

      // created_data: prefix _p_ on each key of each result
      const created_data = results.map(obj => {
        const mapped = {};
        Object.entries(obj || {}).forEach(([k, v]) => { mapped[`_p_${k}`] = v; });
        return mapped;
      });

      const wagwan = properties.object_type;

      return {
        returned_error: false,
        requested_count,
        created_count: successes.length,
        created_ids: successIds,
        created_data,            // API results with _p_ keys
        object_type_text: wagwan,
        created_data_id: successIds
      };
    })
    .catch(err => {
      return {
        returned_error: true,
        error_message: err && err.message ? err.message : "An error occurred while creating items.",
        object_type_text: properties.object_type
      };
    });
}