// Ledgerflow dashboard: plain JavaScript, no build step. It only talks to the public API.
"use strict";

const state = { key: "", accounts: [], selected: null, events: [] };
const $ = (s) => document.querySelector(s);

function store(k, v) { try { v === undefined ? localStorage.removeItem(k) : localStorage.setItem(k, v); } catch (_) {} }
function load(k) { try { return localStorage.getItem(k) || ""; } catch (_) { return ""; } }

function toast(msg, isError) {
  const t = $("#toast");
  t.textContent = msg;
  t.className = "toast" + (isError ? " error" : "");
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => t.classList.add("hidden"), 3500);
}

function money(amount, currency) {
  try { return new Intl.NumberFormat(undefined, { style: "currency", currency, maximumFractionDigits: 0 }).format(amount); }
  catch (_) { return amount + " " + currency; }
}

function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

async function api(method, path, body, idemKey) {
  const headers = { Authorization: "Bearer " + state.key };
  if (body) headers["Content-Type"] = "application/json";
  if (idemKey) headers["Idempotency-Key"] = idemKey;
  const res = await fetch(path, { method, headers, body: body ? JSON.stringify(body) : undefined });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error((data.error && data.error.message) || res.statusText);
    err.status = res.status;
    throw err;
  }
  return { data, replayed: res.headers.get("Idempotent-Replayed") === "true" };
}

// ---------- accounts ----------

async function refreshAccounts() {
  const all = [];
  let cursor = "";
  do {
    const { data } = await api("GET", "/v1/accounts?limit=100" + (cursor ? "&cursor=" + cursor : ""));
    all.push(...data.data);
    cursor = data.next_cursor || "";
  } while (cursor);
  state.accounts = all;
  renderAccounts();
  if (state.selected) renderDetail(state.selected);
}

function renderAccounts() {
  const ul = $("#accounts");
  ul.innerHTML = state.accounts.length ? "" : '<li class="muted">No account yet.</li>';
  for (const a of state.accounts) {
    const li = document.createElement("li");
    li.className = a.id === state.selected ? "active" : "";
    li.innerHTML = `<span>${esc(a.owner_name)} <span class="muted small">${a.currency}</span></span>
                    <span class="amount">${money(a.balance, a.currency)}</span>`;
    li.onclick = () => { state.selected = a.id; renderAccounts(); renderDetail(a.id); };
    ul.appendChild(li);
  }
  const opts = state.accounts.map((a) => `<option value="${a.id}">${esc(a.owner_name)} · ${money(a.balance, a.currency)}</option>`).join("");
  for (const name of ["from", "to"]) {
    const sel = $(`#transfer [name=${name}]`);
    const prev = sel.value;
    sel.innerHTML = opts;
    if (prev) sel.value = prev;
  }
  if (state.accounts.length > 1 && $("#transfer [name=to]").value === $("#transfer [name=from]").value) {
    $("#transfer [name=to]").selectedIndex = 1;
  }
}

async function renderDetail(id) {
  const a = state.accounts.find((x) => x.id === id);
  if (!a) return;
  const { data } = await api("GET", `/v1/accounts/${id}/entries?limit=50`);
  const rows = data.data.map((e) => `<li>
      <span class="muted small">${new Date(e.created_at).toLocaleString()}</span>
      <span class="amount ${e.amount > 0 ? "pos" : "neg"}">${e.amount > 0 ? "+" : ""}${money(e.amount, a.currency)}</span>
    </li>`).join("") || '<li class="muted">No movement yet.</li>';
  $("#detail").innerHTML = `
    <h2>${esc(a.owner_name)}</h2>
    <div class="muted small"><code>${a.id}</code></div>
    <div class="big amount">${money(a.balance, a.currency)}</div>
    <form id="move" class="row">
      <input name="amount" type="number" min="1" step="1" placeholder="Amount" required>
      <button type="submit" name="op" value="deposits">Deposit</button>
      <button type="submit" name="op" value="withdrawals" class="ghost">Withdraw</button>
    </form>
    <h2 style="margin-top:16px">Statement</h2>
    <ul class="entries">${rows}</ul>`;
  $("#move").onsubmit = async (ev) => {
    ev.preventDefault();
    const op = ev.submitter.value;
    const amount = parseInt(ev.target.amount.value, 10);
    try {
      await api("POST", "/v1/" + op, { account_id: id, amount }, crypto.randomUUID());
      toast(op === "deposits" ? "Deposit done" : "Withdrawal done");
      await refreshAccounts();
    } catch (e) { toast(e.message, true); }
  };
}

// ---------- transfers ----------

async function onTransfer(ev) {
  ev.preventDefault();
  const f = ev.target;
  const body = {
    from_account_id: f.from.value, to_account_id: f.to.value,
    amount: parseInt(f.amount.value, 10), reference: f.reference.value,
  };
  const key = crypto.randomUUID(); // one key per user intent; a retry reuses it
  try {
    if (f.double.checked) {
      const [r1, r2] = await Promise.all([api("POST", "/v1/transfers", body, key), api("POST", "/v1/transfers", body, key)]);
      const same = r1.data.id === r2.data.id;
      toast(`2 requests sent, ${same ? "1 transfer created" : "2 transfers?!"} (replayed: ${r1.replayed || r2.replayed})`);
    } else {
      await api("POST", "/v1/transfers", body, key);
      toast("Transfer done");
    }
    f.amount.value = "";
    await refreshAccounts();
  } catch (e) { toast(e.message, true); }
}

// ---------- events ----------

async function pollEvents() {
  if (!state.key) return;
  try {
    const { data } = await api("GET", "/v1/events?order=desc&limit=30");
    state.events = data.data;
    renderEvents();
  } catch (_) { /* ignore transient errors while polling */ }
}

function renderEvents() {
  $("#events").innerHTML = state.events.map((e) => {
    const d = e.data || {};
    return `<li>
      <div class="line"><strong>#${e.sequence} ${esc(e.type)}</strong>
        <span class="badge ${e.delivery_status}">${e.delivery_status.replace("_", " ")}</span></div>
      <div class="line muted small"><span>${esc(d.kind || "")} ${d.amount ? money(d.amount, d.currency) : ""}</span>
        <span>attempts: ${e.delivery_attempts}</span></div>
    </li>`;
  }).join("") || '<li class="muted">No events yet.</li>';
}

// ---------- boot ----------

async function connect(key) {
  state.key = key;
  try {
    await refreshAccounts();
  } catch (e) {
    toast(e.status === 401 ? "Invalid API key" : e.message, true);
    state.key = "";
    return;
  }
  store("ledgerflow.key", key);
  $("#intro").classList.add("hidden");
  $("#app").classList.remove("hidden");
  pollEvents();
}

$("#key-form").onsubmit = (ev) => { ev.preventDefault(); connect($("#api-key").value.trim()); };
$("#new-account").onsubmit = async (ev) => {
  ev.preventDefault();
  const f = ev.target;
  try {
    await api("POST", "/v1/accounts", { owner_name: f.owner_name.value, currency: f.currency.value });
    f.owner_name.value = "";
    await refreshAccounts();
  } catch (e) { toast(e.message, true); }
};
$("#transfer").onsubmit = onTransfer;
setInterval(pollEvents, 2000);

const saved = load("ledgerflow.key");
if (saved) { $("#api-key").value = saved; connect(saved); }
