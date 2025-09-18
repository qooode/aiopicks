"""HTML page rendering for the interactive configuration experience."""

from __future__ import annotations

import json
from textwrap import dedent
from urllib.parse import urlparse

from .config import Settings


CONFIG_TEMPLATE = dedent(
    """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>__APP_NAME__ · Configuration</title>
    <style>
        :root {
            color-scheme: light dark;
            font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif;
            background: #020617;
            color: #e2e8f0;
        }
        * {
            box-sizing: border-box;
        }
        body {
            margin: 0;
            min-height: 100vh;
            background: radial-gradient(circle at top, rgba(30,64,175,0.35), transparent 55%),
                        radial-gradient(circle at 20% 60%, rgba(14,165,233,0.18), transparent 55%),
                        #020617;
        }
        a {
            color: #38bdf8;
        }
        main {
            max-width: 960px;
            margin: 0 auto;
            padding: 3rem 1.5rem 4rem;
        }
        header {
            text-align: center;
            margin-bottom: 2.5rem;
        }
        header h1 {
            margin-bottom: 0.5rem;
            font-size: clamp(2rem, 5vw, 3rem);
            letter-spacing: -0.03em;
        }
        header p {
            margin: 0 auto;
            max-width: 640px;
            color: rgba(226,232,240,0.82);
        }
        .grid {
            display: grid;
            gap: 1.75rem;
        }
        .card {
            background: rgba(15,23,42,0.82);
            border: 1px solid rgba(148,163,184,0.15);
            border-radius: 20px;
            padding: 1.75rem;
            box-shadow: 0 25px 50px -12px rgba(15,23,42,0.45);
            backdrop-filter: blur(18px);
        }
        .card h2 {
            margin-top: 0;
            font-size: 1.4rem;
            margin-bottom: 0.75rem;
        }
        .card p.description {
            margin-top: 0;
            color: rgba(226,232,240,0.72);
            margin-bottom: 1.25rem;
        }
        .field {
            margin-bottom: 1.15rem;
        }
        .field label {
            display: flex;
            justify-content: space-between;
            font-weight: 600;
            font-size: 0.95rem;
            margin-bottom: 0.35rem;
            color: rgba(226,232,240,0.88);
        }
        .field label span.helper {
            font-weight: 400;
            font-size: 0.85rem;
            color: rgba(148,163,184,0.92);
        }
        input[type="text"],
        select {
            width: 100%;
            background: rgba(15,23,42,0.95);
            border: 1px solid rgba(148,163,184,0.25);
            border-radius: 12px;
            color: #e2e8f0;
            padding: 0.65rem 0.85rem;
            font-size: 1rem;
            transition: border 0.2s ease, box-shadow 0.2s ease;
        }
        input[type="text"]:focus,
        select:focus {
            outline: none;
            border-color: rgba(129,140,248,0.8);
            box-shadow: 0 0 0 3px rgba(129,140,248,0.2);
        }
        input[type="range"] {
            width: 100%;
        }
        .range-value {
            font-weight: 600;
            font-size: 0.95rem;
        }
        .actions {
            display: flex;
            gap: 0.75rem;
            flex-wrap: wrap;
            margin-top: 1.25rem;
        }
        button {
            appearance: none;
            border: none;
            border-radius: 999px;
            padding: 0.65rem 1.25rem;
            background: linear-gradient(120deg, #6366f1, #8b5cf6);
            color: white;
            font-weight: 600;
            font-size: 0.95rem;
            cursor: pointer;
            transition: transform 0.15s ease, box-shadow 0.2s ease, filter 0.2s ease;
        }
        button.secondary {
            background: rgba(30,41,59,0.9);
            border: 1px solid rgba(148,163,184,0.25);
        }
        button:hover:not(:disabled) {
            transform: translateY(-1px);
            box-shadow: 0 10px 25px rgba(99,102,241,0.35);
        }
        button:disabled {
            cursor: not-allowed;
            opacity: 0.6;
        }
        .muted {
            font-size: 0.85rem;
            color: rgba(148,163,184,0.9);
        }
        .notice {
            margin-bottom: 1rem;
            padding: 0.75rem 1rem;
            border-radius: 14px;
            background: rgba(15,23,42,0.9);
            border: 1px dashed rgba(148,163,184,0.35);
            color: rgba(226,232,240,0.88);
        }
        .status {
            margin-top: 0.85rem;
            font-size: 0.95rem;
            color: rgba(226,232,240,0.92);
            min-height: 1.2em;
        }
        .status.error {
            color: #fca5a5;
        }
        .status.success {
            color: #86efac;
        }
        .preview {
            background: rgba(15,23,42,0.9);
            border: 1px dashed rgba(148,163,184,0.3);
            border-radius: 14px;
            padding: 0.9rem 1rem;
            margin-top: 1rem;
            font-family: 'JetBrains Mono', ui-monospace, SFMono-Regular, monospace;
            font-size: 0.9rem;
            word-break: break-all;
            color: rgba(226,232,240,0.9);
        }
        .pill {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            background: rgba(30,41,59,0.95);
            border: 1px solid rgba(148,163,184,0.25);
            border-radius: 999px;
            padding: 0.35rem 0.75rem;
            font-size: 0.85rem;
            font-weight: 600;
            color: rgba(226,232,240,0.9);
        }
        .hidden {
            display: none !important;
        }
        @media (max-width: 720px) {
            main {
                padding: 2.5rem 1rem 3rem;
            }
            .actions {
                flex-direction: column;
            }
            button {
                width: 100%;
                justify-content: center;
            }
        }
    </style>
</head>
<body>
    <main>
        <header>
            <p class="pill">Stremio Add-on • __OPENROUTER_MODEL__</p>
            <h1>Configure __APP_NAME__</h1>
            <p>Connect your Trakt account and generate a personalised manifest link for Stremio.</p>
        </header>
        <div class="grid">
            <section class="card" id="trakt-card">
                <h2>Connect Trakt</h2>
                <p class="description">Sign in seamlessly—no device codes or copy/paste hoops required.</p>
                <p class="muted" id="trakt-hint"></p>
                <div class="actions">
                    <button id="trakt-login" type="button">Sign in with Trakt</button>
                    <button id="trakt-disconnect" type="button" class="secondary hidden">Disconnect</button>
                </div>
                <div class="status" id="trakt-status"></div>
            </section>
            <section class="card" id="manifest-card">
                <h2>Manifest builder</h2>
                <p class="description">Choose how many AI generated catalogs to expose and copy a ready-to-install manifest URL. Empty fields fall back to the server defaults.</p>
                <p class="notice hidden" id="manifest-lock">Sign in with Trakt to unlock personalised manifest links.</p>
                <div class="field">
                    <label for="config-openrouter-key">OpenRouter API key <span class="helper">Optional – stored client side only</span></label>
                    <input id="config-openrouter-key" type="text" placeholder="sk-or-..." autocomplete="off" spellcheck="false" />
                </div>
                <div class="field">
                    <label for="config-openrouter-model">Model <span class="helper">Default: __OPENROUTER_MODEL__</span></label>
                    <input id="config-openrouter-model" type="text" placeholder="google/gemini-2.5-flash-lite" />
                </div>
                <div class="field">
                    <label for="config-catalog-count">Catalog rows <span class="range-value" id="catalog-count-value"></span></label>
                    <input id="config-catalog-count" type="range" min="1" max="12" step="1" />
                </div>
                <div class="field">
                    <label for="config-refresh-interval">Refresh cadence <span class="helper">How often Gemini rethinks the catalogs</span></label>
                    <select id="config-refresh-interval">
                        <option value="3600">Every hour</option>
                        <option value="14400">Every 4 hours</option>
                        <option value="43200">Every 12 hours</option>
                        <option value="86400">Every 24 hours</option>
                    </select>
                </div>
                <div class="field">
                    <label for="config-cache-ttl">Response cache <span class="helper">Stremio responses stay fresh for...</span></label>
                    <select id="config-cache-ttl">
                        <option value="300">5 minutes</option>
                        <option value="900">15 minutes</option>
                        <option value="1800">30 minutes</option>
                        <option value="3600">60 minutes</option>
                    </select>
                </div>
                <div class="actions">
                    <button id="copy-default-manifest" type="button">Copy public manifest</button>
                    <button id="copy-configured-manifest" type="button" class="secondary">Copy configured manifest</button>
                </div>
                <p class="muted" id="copy-message"></p>
                <div class="preview" id="manifest-preview"></div>
            </section>
        </div>
    </main>
    <script>
        (function () {
            const defaults = JSON.parse('__DEFAULTS_JSON__');
            const baseManifestUrl = new URL('/manifest.json', window.location.origin).toString();

            const openrouterKey = document.getElementById('config-openrouter-key');
            const openrouterModel = document.getElementById('config-openrouter-model');
            const catalogSlider = document.getElementById('config-catalog-count');
            const catalogValue = document.getElementById('catalog-count-value');
            const refreshSelect = document.getElementById('config-refresh-interval');
            const cacheSelect = document.getElementById('config-cache-ttl');
            const copyDefaultManifest = document.getElementById('copy-default-manifest');
            const copyConfiguredManifest = document.getElementById('copy-configured-manifest');
            const manifestPreview = document.getElementById('manifest-preview');
            const copyMessage = document.getElementById('copy-message');
            const manifestLock = document.getElementById('manifest-lock');

            const traktLoginButton = document.getElementById('trakt-login');
            const traktDisconnectButton = document.getElementById('trakt-disconnect');
            const traktStatus = document.getElementById('trakt-status');
            const traktHint = document.getElementById('trakt-hint');
            const traktLoginAvailable = Boolean(defaults.traktLoginAvailable);
            const traktCallbackOrigin = (defaults.traktCallbackOrigin || '').trim();
            const traktOrigins = [window.location.origin];
            if (traktCallbackOrigin && !traktOrigins.includes(traktCallbackOrigin)) {
                traktOrigins.push(traktCallbackOrigin);
            }

            const traktAuth = { accessToken: '', refreshToken: '' };
            let traktPending = false;
            let copyTimeout = null;

            openrouterModel.value = defaults.openrouterModel || '';
            catalogSlider.value = defaults.catalogCount || catalogSlider.min || 1;
            catalogValue.textContent = catalogSlider.value;
            refreshSelect.value = String(defaults.refreshIntervalSeconds || refreshSelect.value);
            ensureOption(refreshSelect, refreshSelect.value, formatSeconds(Number(refreshSelect.value)));
            cacheSelect.value = String(defaults.responseCacheSeconds || cacheSelect.value);
            ensureOption(cacheSelect, cacheSelect.value, formatSeconds(Number(cacheSelect.value)));

            const storedTokens = readStoredTokens();
            if (storedTokens && storedTokens.access_token) {
                applyTraktTokens(storedTokens, { silent: true });
            } else if ((defaults.traktAccessToken || '').trim()) {
                applyTraktTokens({ access_token: defaults.traktAccessToken }, { silent: true });
            } else {
                updateTraktUi();
            }
            refreshTraktMessaging();
            updateManifestPreview();

            catalogSlider.addEventListener('input', () => {
                catalogValue.textContent = catalogSlider.value;
                updateManifestPreview();
            });
            openrouterModel.addEventListener('input', updateManifestPreview);
            openrouterKey.addEventListener('input', updateManifestPreview);
            refreshSelect.addEventListener('change', () => {
                ensureOption(refreshSelect, refreshSelect.value, formatSeconds(Number(refreshSelect.value)));
                updateManifestPreview();
            });
            cacheSelect.addEventListener('change', () => {
                ensureOption(cacheSelect, cacheSelect.value, formatSeconds(Number(cacheSelect.value)));
                updateManifestPreview();
            });

            copyDefaultManifest.addEventListener('click', async () => {
                await copyToClipboard(baseManifestUrl);
                setCopyMessage('Base manifest URL copied to clipboard.');
            });

            copyConfiguredManifest.addEventListener('click', async () => {
                const url = buildConfiguredUrl();
                await copyToClipboard(url);
                setCopyMessage('Configured manifest URL copied. Install it in Stremio to use your settings.');
            });

            traktLoginButton.addEventListener('click', async () => {
                if (!traktLoginAvailable || traktPending) {
                    return;
                }
                traktPending = true;
                updateTraktUi();
                showTraktStatus('Opening Trakt sign in…');
                showTraktHint('A secure pop-up will appear. Approve access in Trakt and this page will update automatically.');
                try {
                    const response = await fetch('/api/trakt/login-url', { method: 'POST' });
                    const payload = await response.json().catch(() => ({}));
                    if (!response.ok || !payload.url) {
                        traktPending = false;
                        updateTraktUi();
                        showTraktStatus(resolveErrorMessage(payload) || 'Unable to start Trakt sign in.', 'error');
                        showTraktHint('Please try again in a moment.');
                        return;
                    }
                    const popup = window.open(payload.url, 'trakt-sign-in', 'width=600,height=780');
                    if (!popup) {
                        traktPending = false;
                        updateTraktUi();
                        showTraktStatus('Allow pop-ups for this site to continue with Trakt sign in.', 'error');
                        showTraktHint('After enabling pop-ups, click “Sign in with Trakt” again.');
                        return;
                    }
                    popup.focus();
                } catch (error) {
                    console.error(error);
                    traktPending = false;
                    updateTraktUi();
                    showTraktStatus('Could not reach the server to start Trakt sign in.', 'error');
                    showTraktHint('Check your connection and try again.');
                }
            });

            traktDisconnectButton.addEventListener('click', () => {
                if (traktPending) {
                    return;
                }
                traktAuth.accessToken = '';
                traktAuth.refreshToken = '';
                persistTraktTokens(null);
                updateManifestPreview();
                updateTraktUi();
                showTraktStatus('Trakt disconnected. Sign in again to reconnect.');
                showTraktHint('');
            });

            let traktBroadcastChannel = null;

            function normalizeTraktOauthPayload(rawPayload) {
                if (rawPayload == null) {
                    return null;
                }
                let data = rawPayload;
                if (typeof data === 'string') {
                    try {
                        data = JSON.parse(data);
                    } catch (err) {
                        console.warn('Ignoring malformed Trakt OAuth payload', err);
                        return null;
                    }
                }
                if (!data || typeof data !== 'object') {
                    return null;
                }
                if (data.source === 'trakt-oauth') {
                    if (!data.tokens && (data.access_token || data.refresh_token)) {
                        data.tokens = {
                            access_token: data.access_token || data.accessToken || '',
                            refresh_token: data.refresh_token || data.refreshToken || '',
                        };
                    }
                    return data;
                }
                const type = typeof data.type === 'string' ? data.type.toUpperCase() : '';
                if (type === 'TRAKT_AUTH_SUCCESS') {
                    const tokens = {
                        access_token: data.access_token || data.accessToken || '',
                        refresh_token: data.refresh_token || data.refreshToken || '',
                    };
                    if (data.expires_in != null) {
                        tokens.expires_in = data.expires_in;
                    }
                    if (data.scope) {
                        tokens.scope = data.scope;
                    }
                    if (data.token_type) {
                        tokens.token_type = data.token_type;
                    }
                    return {
                        source: 'trakt-oauth',
                        status: 'success',
                        tokens,
                    };
                }
                if (type === 'TRAKT_AUTH_ERROR') {
                    return {
                        source: 'trakt-oauth',
                        status: 'error',
                        error: data.error || data.message || 'trakt_error',
                        error_description: data.error_description || data.description || '',
                    };
                }
                return null;
            }

            function handleTraktOauthPayload(rawPayload, origin) {
                const data = normalizeTraktOauthPayload(rawPayload);
                if (!data || data.source !== 'trakt-oauth') {
                    return;
                }
                if (origin && !traktOrigins.includes(origin)) {
                    return;
                }
                traktPending = false;
                applyTraktTokens(data.tokens || {}, { silent: true });
                updateTraktUi();
                if (data.status === 'success' && traktAuth.accessToken) {
                    refreshTraktMessaging();
                } else if (data.status === 'success') {
                    showTraktStatus('Trakt did not return an access token. Please try again.', 'error');
                    showTraktHint('Approve the access request in the Trakt window to finish linking.');
                } else {
                    showTraktStatus(
                        data.error_description || data.error || 'Trakt rejected the sign in request.',
                        'error'
                    );
                    showTraktHint('Try again and ensure you approve the request in Trakt.');
                }
            }

            window.addEventListener('message', (event) => {
                handleTraktOauthPayload(event.data, event.origin || '');
            });

            if ('BroadcastChannel' in window) {
                traktBroadcastChannel = new BroadcastChannel('aiopicks.trakt-oauth');
                traktBroadcastChannel.addEventListener('message', (event) => {
                    handleTraktOauthPayload(event.data, '');
                });
            }

            window.addEventListener('beforeunload', () => {
                if (traktBroadcastChannel) {
                    traktBroadcastChannel.close();
                }
            });

            function setCopyMessage(message) {
                copyMessage.textContent = message;
                if (copyTimeout) {
                    clearTimeout(copyTimeout);
                }
                if (message) {
                    copyTimeout = setTimeout(() => {
                        copyMessage.textContent = '';
                    }, 6000);
                }
            }

            function buildConfiguredUrl() {
                const url = new URL(baseManifestUrl);
                const params = new URLSearchParams();
                const key = openrouterKey.value.trim();
                if (key) params.set('openrouterKey', key);
                const model = openrouterModel.value.trim();
                if (model) params.set('openrouterModel', model);
                const count = catalogSlider.value;
                if (count) params.set('catalogCount', count);
                const refresh = refreshSelect.value;
                if (refresh) params.set('refreshInterval', refresh);
                const cache = cacheSelect.value;
                if (cache) params.set('cacheTtl', cache);
                if (traktAuth.accessToken) {
                    params.set('traktAccessToken', traktAuth.accessToken);
                }
                url.search = params.toString();
                return url.toString();
            }

            function updateManifestPreview() {
                manifestPreview.textContent = buildConfiguredUrl();
                copyConfiguredManifest.disabled = traktLoginAvailable && !traktAuth.accessToken;
                manifestLock.classList.toggle('hidden', !traktLoginAvailable || Boolean(traktAuth.accessToken));
            }

            async function copyToClipboard(value) {
                try {
                    await navigator.clipboard.writeText(value);
                } catch (err) {
                    const textarea = document.createElement('textarea');
                    textarea.value = value;
                    textarea.setAttribute('readonly', '');
                    textarea.style.position = 'absolute';
                    textarea.style.left = '-9999px';
                    document.body.appendChild(textarea);
                    textarea.select();
                    document.execCommand('copy');
                    document.body.removeChild(textarea);
                }
            }

            function ensureOption(select, value, label) {
                const exists = Array.from(select.options).some((option) => option.value === value);
                if (!exists) {
                    const option = document.createElement('option');
                    option.value = value;
                    option.textContent = `Custom (${label})`;
                    select.appendChild(option);
                }
            }

            function formatSeconds(seconds) {
                if (!Number.isFinite(seconds) || seconds <= 0) {
                    return 'custom';
                }
                if (seconds % 3600 === 0) {
                    const hours = seconds / 3600;
                    return hours === 1 ? '1 hour' : `${hours} hours`;
                }
                if (seconds % 60 === 0) {
                    const minutes = seconds / 60;
                    return minutes === 1 ? '1 minute' : `${minutes} minutes`;
                }
                return `${seconds} seconds`;
            }

            function updateTraktUi() {
                const connected = Boolean(traktAuth.accessToken);
                traktLoginButton.classList.toggle('hidden', !traktLoginAvailable || connected);
                traktDisconnectButton.classList.toggle('hidden', !connected);
                traktLoginButton.disabled = !traktLoginAvailable || traktPending;
                traktDisconnectButton.disabled = traktPending || !connected;
            }

            function refreshTraktMessaging() {
                if (!traktLoginAvailable) {
                    showTraktStatus(
                        'Server is not configured with Trakt credentials. Ask the administrator to set TRAKT_CLIENT_ID and TRAKT_CLIENT_SECRET.',
                        'error'
                    );
                    showTraktHint('');
                    return;
                }
                if (traktPending) {
                    showTraktStatus('Waiting for you to finish signing in on Trakt…');
                    showTraktHint('');
                    return;
                }
                if (traktAuth.accessToken) {
                    showTraktStatus('Trakt connected! Manifest links now include your access token automatically.', 'success');
                    if (traktAuth.refreshToken) {
                        showTraktHint('Your tokens are stored locally in this browser so you stay signed in next time.');
                    } else {
                        showTraktHint('Your access token is stored locally in this browser for manifest generation.');
                    }
                    return;
                }
                showTraktStatus('Sign in to Trakt to unlock personalised recommendations.');
                showTraktHint('A secure pop-up will open and you simply approve access—no codes required.');
            }

            function applyTraktTokens(tokens, options = {}) {
                const access = ((tokens && (tokens.access_token || tokens.accessToken)) || '').trim();
                const refresh = ((tokens && (tokens.refresh_token || tokens.refreshToken)) || '').trim();
                traktAuth.accessToken = access;
                traktAuth.refreshToken = refresh;
                persistTraktTokens(access ? { access_token: access, refresh_token: refresh } : null);
                updateTraktUi();
                if (!options.silent) {
                    if (access) {
                        refreshTraktMessaging();
                    } else {
                        showTraktStatus('Trakt tokens cleared. Sign in again to reconnect.');
                        showTraktHint('');
                    }
                }
                updateManifestPreview();
            }

            function persistTraktTokens(tokens) {
                try {
                    if (tokens && tokens.access_token) {
                        localStorage.setItem('aiopicks.traktTokens', JSON.stringify(tokens));
                    } else {
                        localStorage.removeItem('aiopicks.traktTokens');
                    }
                } catch (err) {
                    console.warn('Unable to persist Trakt tokens', err);
                }
            }

            function readStoredTokens() {
                try {
                    const raw = localStorage.getItem('aiopicks.traktTokens');
                    if (!raw) {
                        return null;
                    }
                    const parsed = JSON.parse(raw);
                    if (parsed && typeof parsed === 'object') {
                        return parsed;
                    }
                } catch (err) {
                    console.warn('Unable to read stored Trakt tokens', err);
                }
                return null;
            }

            function showTraktStatus(message, variant) {
                traktStatus.textContent = message;
                traktStatus.classList.toggle('success', variant === 'success');
                traktStatus.classList.toggle('error', variant === 'error');
                if (variant !== 'success' && variant !== 'error' && message) {
                    traktStatus.classList.remove('success');
                    traktStatus.classList.remove('error');
                }
                if (!message) {
                    traktStatus.classList.remove('success');
                    traktStatus.classList.remove('error');
                }
            }

            function showTraktHint(message) {
                traktHint.textContent = message;
            }

            function resolveErrorMessage(payload) {
                if (!payload) {
                    return null;
                }
                if (typeof payload.detail === 'string') {
                    return payload.detail;
                }
                if (payload.detail && typeof payload.detail === 'object') {
                    return payload.detail.description || payload.detail.error || null;
                }
                return payload.message || payload.error_description || payload.error || null;
            }
        })();
    </script>
</body>
</html>
    """
)


def render_config_page(settings: Settings, *, callback_origin: str = "") -> str:
    """Return the full HTML for the `/config` landing page."""

    resolved_callback_origin = ""
    candidate_origin = callback_origin.strip()
    if candidate_origin:
        resolved_callback_origin = candidate_origin.rstrip("/")
    elif settings.trakt_redirect_uri:
        parsed = urlparse(str(settings.trakt_redirect_uri))
        if parsed.scheme and parsed.netloc:
            resolved_callback_origin = (
                f"{parsed.scheme}://{parsed.netloc}"
            ).rstrip("/")

    defaults = {
        "appName": settings.app_name,
        "openrouterModel": settings.openrouter_model,
        "catalogCount": settings.catalog_count,
        "refreshIntervalSeconds": settings.refresh_interval_seconds,
        "responseCacheSeconds": settings.response_cache_seconds,
        "traktAccessToken": settings.trakt_access_token or "",
        "traktLoginAvailable": bool(
            settings.trakt_client_id and settings.trakt_client_secret
        ),
        "traktCallbackOrigin": resolved_callback_origin,
    }
    defaults_json = json.dumps(defaults).replace("</", "<\\/")

    html = CONFIG_TEMPLATE
    replacements = {
        "__APP_NAME__": settings.app_name,
        "__OPENROUTER_MODEL__": settings.openrouter_model,
        "__DEFAULTS_JSON__": defaults_json,
    }
    for placeholder, value in replacements.items():
        html = html.replace(placeholder, value)
    return html
