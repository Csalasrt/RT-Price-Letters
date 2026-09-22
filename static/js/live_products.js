/*
 * Live product list
 * -----------------
 * Keeps every product dropdown in the app in sync with the master
 * Products list without a page reload.
 *
 * How it works:
 *   - base.html sets window.LIVE_PRODUCTS_CONFIG = { version, versionUrl, listUrl }
 *     where `version` is the fingerprint of the Products list at render time.
 *   - This script polls versionUrl (cheap) every few seconds while the tab
 *     is visible, and immediately whenever the tab regains focus.
 *   - Tabs in the same browser also tell each other about new versions via
 *     BroadcastChannel, so a change made on the Products page shows up in
 *     other open tabs right away instead of waiting for the next poll.
 *   - When the version changes, the fresh list is fetched once and handed
 *     to every page-level subscriber, which rebuilds its own dropdown.
 *
 * Page usage:
 *   LiveProducts.subscribe(function (products, info) { ...rebuild... });
 *   LiveProducts.notifyChanged();   // after this page changed the list
 *   LiveProducts.fetchJSON(url);    // helper for page-specific extras
 */
(function () {
  "use strict";

  var cfg = window.LIVE_PRODUCTS_CONFIG || {};
  if (!cfg.versionUrl || !cfg.listUrl) {
    // Not logged in (login page etc.) - expose a no-op API so page scripts
    // can call it unconditionally.
    window.LiveProducts = {
      subscribe: function () {},
      notifyChanged: function () {},
      refreshNow: function () {},
      fetchJSON: function () { return Promise.reject(new Error("disabled")); },
      getVersion: function () { return ""; }
    };
    return;
  }

  var POLL_MS = cfg.pollMs || 5000;
  var CHANNEL_NAME = "rt-pl-live-products";

  var currentVersion = cfg.version || "";
  var subscribers = [];
  var inflightCheck = null;
  var channel = null;

  try {
    if ("BroadcastChannel" in window) channel = new BroadcastChannel(CHANNEL_NAME);
  } catch (e) {
    channel = null;
  }

  function fetchJSON(url) {
    return fetch(url, {
      method: "GET",
      credentials: "same-origin",
      cache: "no-store",
      headers: { "Accept": "application/json" }
    }).then(function (res) {
      if (!res.ok) throw new Error("HTTP " + res.status);
      return res.json();
    });
  }

  function broadcast(version) {
    if (!channel || !version) return;
    try { channel.postMessage({ type: "products-version", version: version }); } catch (e) {}
  }

  function deliver(data) {
    var products = Array.isArray(data.products) ? data.products : [];
    subscribers.forEach(function (fn) {
      try {
        fn(products, { version: data.version });
      } catch (err) {
        if (window.console) console.error("LiveProducts subscriber failed:", err);
      }
    });
  }

  // Fetch the full list and push it to subscribers. `force` skips the
  // "same version" short-circuit (used after this tab changed something).
  function pullList(force) {
    return fetchJSON(cfg.listUrl).then(function (data) {
      if (!data || !data.ok) return;
      var changed = data.version !== currentVersion;
      currentVersion = data.version;
      if (changed || force) {
        deliver(data);
        broadcast(currentVersion);
      }
    });
  }

  function check(force) {
    if (inflightCheck) return inflightCheck;

    var p = force
      ? pullList(true)
      : fetchJSON(cfg.versionUrl).then(function (data) {
          if (data && data.ok && data.version && data.version !== currentVersion) {
            return pullList(false);
          }
        });

    inflightCheck = p
      .catch(function () { /* offline / session expired - try again next tick */ })
      .then(function () { inflightCheck = null; });

    return inflightCheck;
  }

  if (channel) {
    channel.onmessage = function (ev) {
      var msg = ev && ev.data;
      if (!msg || msg.type !== "products-version") return;
      if (msg.version && msg.version !== currentVersion) check(false);
    };
  }

  setInterval(function () {
    if (document.visibilityState === "hidden") return;
    check(false);
  }, POLL_MS);

  document.addEventListener("visibilitychange", function () {
    if (document.visibilityState === "visible") check(false);
  });
  window.addEventListener("focus", function () { check(false); });

  // Coming back via the browser Back button can restore a cached page with
  // an old list baked in - re-check straight away in that case.
  window.addEventListener("pageshow", function (ev) {
    if (ev.persisted) check(false);
  });

  // Let other open tabs know what version this freshly loaded page saw.
  // If this page load came right after a change (e.g. the Products page
  // form just saved), other tabs pick it up immediately.
  broadcast(currentVersion);

  // ── Built-in handling for plain markup ──
  // Any <select data-live-products> or <datalist data-live-products> is
  // rebuilt automatically, so simple dropdowns don't need their own script.
  //   - <option data-static> entries (e.g. "All Products") stay at the top.
  //   - A <select>'s current choice is kept; if that product is no longer
  //     on the list it stays visible (marked) instead of silently flipping
  //     to something else.
  function productNames(products) {
    var seen = {};
    var names = [];
    products.forEach(function (p) {
      var name = String((p && (typeof p === "string" ? p : p.product)) || "").trim();
      var key = name.toLowerCase().split(/\s+/).join(" ");
      if (name && !seen[key]) {
        seen[key] = true;
        names.push(name);
      }
    });
    names.sort(function (a, b) { return a.toLowerCase().localeCompare(b.toLowerCase()); });
    return names;
  }

  function rebuildSelect(sel, names) {
    var current = sel.value;
    var statics = Array.prototype.filter.call(sel.options, function (o) {
      return o.hasAttribute("data-static");
    });

    sel.innerHTML = "";
    statics.forEach(function (o) { sel.appendChild(o); });

    var found = false;
    names.forEach(function (name) {
      var opt = document.createElement("option");
      opt.value = name;
      opt.textContent = name;
      if (name === current) { opt.selected = true; found = true; }
      sel.appendChild(opt);
    });

    var isStatic = statics.some(function (o) { return o.value === current; });
    if (current && !found && !isStatic) {
      var keep = document.createElement("option");
      keep.value = current;
      keep.textContent = current + " (no longer on Products list)";
      keep.selected = true;
      sel.insertBefore(keep, sel.options[statics.length] || null);
    } else if (!current || isStatic) {
      sel.value = current;
    }
  }

  function rebuildDatalist(list, names) {
    list.innerHTML = names.map(function (name) {
      var opt = document.createElement("option");
      opt.value = name;
      return opt.outerHTML;
    }).join("");
  }

  subscribers.push(function (products) {
    var names = productNames(products);
    document.querySelectorAll("select[data-live-products]").forEach(function (sel) {
      rebuildSelect(sel, names);
    });
    document.querySelectorAll("datalist[data-live-products]").forEach(function (list) {
      rebuildDatalist(list, names);
    });
  });

  window.LiveProducts = {
    subscribe: function (fn) {
      if (typeof fn === "function") subscribers.push(fn);
    },
    notifyChanged: function () { return check(true); },
    refreshNow: function () { return check(false); },
    fetchJSON: fetchJSON,
    getVersion: function () { return currentVersion; }
  };
})();