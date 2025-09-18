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
            --page: #f4f5f7;
            --surface: #fcfcfd;
            --surface-alt: #f0f1f3;
            --surface-strong: #e4e7ec;
            --text-primary: #0f172a;
            --text-muted: #5d6675;
            --outline: #d7dbe3;
            --outline-strong: #b4bcc9;
            --shadow-soft: rgba(15, 23, 42, 0.1);
            --shadow-strong: rgba(15, 23, 42, 0.18);
            --accent: #131c2b;
            --accent-contrast: #f6f7f9;
            --focus-ring: rgba(56, 72, 99, 0.22);
            --step-size: 3.25rem;
        }
        @media (prefers-color-scheme: dark) {
            :root {
                --page: #050914;
                --surface: rgba(15, 23, 42, 0.72);
                --surface-alt: rgba(31, 41, 59, 0.72);
                --surface-strong: rgba(52, 66, 86, 0.68);
                --text-primary: #e2e8f0;
                --text-muted: #9aa4b7;
                --outline: rgba(148, 163, 184, 0.4);
                --outline-strong: rgba(148, 163, 184, 0.66);
                --shadow-soft: rgba(2, 6, 23, 0.42);
                --shadow-strong: rgba(2, 6, 23, 0.6);
                --accent: #e2e8f0;
                --accent-contrast: #050914;
                --focus-ring: rgba(148, 163, 184, 0.25);
            }
        }
        * {
            box-sizing: border-box;
        }
        @media (prefers-reduced-motion: reduce) {
            *,
            *::before,
            *::after {
                animation-duration: 0.01ms !important;
                animation-iteration-count: 1 !important;
                transition-duration: 0.01ms !important;
                scroll-behavior: auto !important;
            }
        }
        body {
            margin: 0;
            min-height: 100vh;
            background: var(--page);
            color: var(--text-primary);
            line-height: 1.65;
            -webkit-font-smoothing: antialiased;
            padding: 0 1.75rem 4rem;
        }
        a {
            color: inherit;
            text-decoration: underline;
            text-decoration-color: var(--outline-strong);
            text-decoration-thickness: 2px;
        }
        a:hover {
            text-decoration-color: var(--accent);
        }
        main {
            width: min(820px, 100%);
            margin: 0 auto;
            padding: 4rem 0 5rem;
            display: flex;
            flex-direction: column;
            gap: 3rem;
        }
        header {
            position: relative;
            display: flex;
            flex-direction: column;
            gap: 1.25rem;
            padding: 2.75rem 3rem;
            border-radius: 28px;
            background: var(--surface);
            border: 1px solid var(--outline);
            box-shadow: 0 32px 72px -56px var(--shadow-strong);
            overflow: hidden;
        }
        header h1 {
            margin: 0;
            font-size: clamp(2.1rem, 5vw, 3.2rem);
            letter-spacing: -0.03em;
            font-weight: 650;
        }
        header p {
            margin: 0;
            max-width: 640px;
            color: var(--text-muted);
            font-size: 1.02rem;
        }
        header::after {
            content: '';
            position: absolute;
            inset: 0;
            border-radius: inherit;
            pointer-events: none;
            box-shadow: inset 0 0 0 1px var(--outline);
            opacity: 0.6;
        }
        .pill {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            font-size: 0.8rem;
            font-weight: 600;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            padding: 0.45rem 0.9rem;
            border-radius: 999px;
            border: 1px solid var(--outline-strong);
            background: var(--surface-alt);
            color: var(--text-muted);
        }
        .status-board {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 0.75rem;
            padding: 0;
            margin: 0;
            list-style: none;
        }
        .status-board li {
            display: flex;
            flex-direction: column;
            gap: 0.25rem;
            padding: 0.85rem 1rem;
            border-radius: 18px;
            border: 1px solid var(--outline);
            background: var(--surface-alt);
        }
        .status-board .label {
            text-transform: uppercase;
            font-size: 0.72rem;
            letter-spacing: 0.12em;
            color: var(--text-muted);
            font-weight: 600;
        }
        .status-board .value {
            font-weight: 600;
            font-size: 0.98rem;
        }
        .flow {
            position: relative;
            display: flex;
            flex-direction: column;
            gap: 3rem;
            padding-left: calc(var(--step-size) + 1.5rem);
        }
        .flow::before {
            content: '';
            position: absolute;
            top: 1.5rem;
            bottom: 1.5rem;
            left: calc(var(--step-size) / 2);
            width: 2px;
            background: var(--outline);
        }
        .card {
            position: relative;
            display: flex;
            flex-direction: column;
            gap: 1.35rem;
            background: var(--surface);
            border: 1px solid var(--outline);
            border-radius: 26px;
            padding: 2.75rem 2.75rem 2.5rem;
            box-shadow: 0 28px 52px -44px var(--shadow-soft);
        }
        .card::after {
            content: '';
            position: absolute;
            inset: 0;
            border-radius: inherit;
            pointer-events: none;
            box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.08);
        }
        .card.step {
            padding-left: calc(2.75rem + var(--step-size));
        }
        .card-head {
            display: flex;
            gap: 1.5rem;
            align-items: flex-start;
        }
        .step-marker {
            position: absolute;
            left: calc(var(--step-size) / 2 - 1.75rem);
            top: 2.4rem;
            width: var(--step-size);
            height: var(--step-size);
            border-radius: 50%;
            border: 2px solid var(--outline-strong);
            background: var(--surface);
            display: grid;
            place-items: center;
            font-weight: 600;
            font-size: 1.05rem;
            color: var(--text-primary);
            box-shadow: 0 18px 36px -26px var(--shadow-strong);
        }
        .card.step:last-child::before {
            content: '';
            position: absolute;
            bottom: -1.5rem;
            left: calc(var(--step-size) / 2 - 1px);
            width: 2px;
            height: 1.5rem;
            background: var(--page);
        }
        .card h2 {
            margin: 0;
            font-size: 1.4rem;
            letter-spacing: -0.01em;
            font-weight: 650;
        }
        .card p.description {
            margin: 0;
            color: var(--text-muted);
            font-size: 0.98rem;
            line-height: 1.6;
            max-width: 540px;
        }
        .field {
            display: flex;
            flex-direction: column;
            gap: 0.35rem;
        }
        .field label {
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            gap: 1rem;
            font-weight: 600;
            font-size: 0.95rem;
            color: var(--text-primary);
        }
        .field label span.helper {
            font-weight: 400;
            font-size: 0.85rem;
            color: var(--text-muted);
        }
        input[type="text"],
        select {
            width: 100%;
            background: var(--surface-alt);
            border: 1px solid var(--outline);
            border-radius: 16px;
            color: var(--text-primary);
            padding: 0.7rem 0.9rem;
            font-size: 1rem;
            transition: border-color 0.2s ease, box-shadow 0.2s ease, background-color 0.2s ease;
            font-variant-numeric: tabular-nums;
        }
        input[type="text"]:hover,
        select:hover {
            border-color: var(--outline-strong);
        }
        input[type="text"]:focus-visible,
        select:focus-visible {
            outline: none;
            border-color: var(--accent);
            box-shadow: 0 0 0 4px var(--focus-ring);
            background: var(--surface);
        }
        input[type="range"] {
            width: 100%;
            accent-color: var(--accent);
        }
        .range-value {
            font-weight: 600;
            font-size: 0.95rem;
            color: var(--text-muted);
        }
        .actions {
            display: flex;
            flex-wrap: wrap;
            gap: 0.75rem;
            margin-top: 0.5rem;
        }
        button {
            appearance: none;
            border: 1px solid transparent;
            border-radius: 18px;
            padding: 0.82rem 1.75rem;
            background: var(--accent);
            color: var(--accent-contrast);
            font-weight: 600;
            font-size: 0.95rem;
            letter-spacing: 0.01em;
            cursor: pointer;
            transition: transform 0.15s ease, box-shadow 0.2s ease, filter 0.2s ease;
            box-shadow: 0 18px 36px -28px var(--shadow-strong);
            text-transform: none;
        }
        button:hover:not(:disabled) {
            transform: translateY(-1px);
            box-shadow: 0 18px 36px -28px var(--shadow-strong);
            filter: brightness(1.05);
        }
        button:focus-visible {
            outline: none;
            box-shadow: 0 0 0 4px var(--focus-ring);
        }
        button.secondary {
            background: var(--surface);
            color: var(--text-primary);
            border-color: var(--outline);
            box-shadow: none;
        }
        button.secondary:hover:not(:disabled) {
            filter: none;
            background: var(--surface-alt);
            border-color: var(--outline-strong);
        }
        button:disabled {
            cursor: not-allowed;
            opacity: 0.55;
            transform: none;
            box-shadow: none;
            filter: none;
        }
        .muted {
            font-size: 0.88rem;
            color: var(--text-muted);
        }
        .notice {
            margin-bottom: 1rem;
            padding: 0.95rem 1.1rem;
            border-radius: 18px;
            background: var(--surface-alt);
            border: 1px solid var(--outline);
            color: var(--text-primary);
            box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.12);
        }
        .status {
            margin-top: 0.85rem;
            font-size: 0.95rem;
            color: var(--text-primary);
            min-height: 1.2em;
            padding: 0.5rem 0.75rem;
            border-radius: 12px;
            background: var(--surface-alt);
            border-left: 3px solid transparent;
            box-shadow: inset 0 0 0 1px var(--outline);
        }
        .status.error {
            border-left-color: var(--outline-strong);
        }
        .status.success {
            border-left-color: var(--outline);
        }
        .preview {
            background: var(--surface-alt);
            border: 1px solid var(--outline);
            border-radius: 16px;
            padding: 1rem 1.1rem;
            margin-top: 1rem;
            font-family: 'JetBrains Mono', ui-monospace, SFMono-Regular, monospace;
            font-size: 0.9rem;
            word-break: break-word;
            color: var(--text-primary);
            box-shadow: inset 0 0 0 1px rgba(15, 23, 42, 0.04);
        }
        .hidden {
            display: none !important;
        }
        @media (max-width: 720px) {
            body {
                padding: 0 1.25rem;
            }
            main {
                padding: 3rem 0 3.5rem;
                gap: 2.25rem;
            }
            header {
                padding: 2.25rem 2rem;
            }
            .status-board {
                grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            }
            .flow {
                padding-left: 0;
            }
            .flow::before {
                display: none;
            }
            .card {
                padding: 2.2rem 2rem;
            }
            .card.step {
                padding-left: 2rem;
            }
            .step-marker {
                position: static;
                width: 2.5rem;
                height: 2.5rem;
                margin-bottom: 1rem;
                box-shadow: none;
            }
            .card-head {
                flex-direction: column;
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
                <p class="pill">Stremio Add-on</p>
                <h1>Configure __APP_NAME__</h1>
                <p>Connect your Trakt account, adjust the catalog settings, and copy an install-ready link for Stremio.</p>
                <ul class="status-board">
                    <li>
                        <span class="label">Step one</span>
                        <span class="value">Authorize Trakt</span>
                    </li>
                    <li>
                        <span class="label">Step two</span>
                        <span class="value">Shape your catalogs</span>
                    </li>
                    <li>
                        <span class="label">Result</span>
                        <span class="value">Share the manifest link</span>
                    </li>
                </ul>
            </header>
            <div class="flow">
                <section class="card step" data-step="1" id="trakt-card">
                    <div class="card-head">
                        <span class="step-marker" aria-hidden="true">1</span>
                        <div>
                            <h2>Connect Trakt</h2>
                            <p class="description">Sign in seamlessly—no device codes or copy/paste hoops required.</p>
                        </div>
                    </div>
                    <p class="muted" id="trakt-hint"></p>
                    <div class="actions">
                        <button id="trakt-login" type="button">Sign in with Trakt</button>
                        <button id="trakt-disconnect" type="button" class="secondary hidden">Disconnect</button>
                    </div>
                    <div class="status" id="trakt-status"></div>
                </section>
                <section class="card step" data-step="2" id="manifest-card">
                    <div class="card-head">
                        <span class="step-marker" aria-hidden="true">2</span>
                        <div>
                            <h2>Manifest builder</h2>
                            <p class="description">Choose how many AI generated catalogs to expose and copy a ready-to-install manifest URL. Empty fields fall back to the server defaults.</p>
                        </div>
                    </div>
                    <p class="notice hidden" id="manifest-lock">Sign in with Trakt to unlock personalised manifest links.</p>
                    <div class="field">
                        <label>Add-on name <span class="helper">Fixed to __APP_NAME__</span></label>
                        <p class="muted">Stremio will always display this add-on as <strong>__APP_NAME__</strong>.</p>
                    </div>
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
                        <label for="config-catalog-items">Items per catalog <span class="range-value" id="catalog-items-value"></span></label>
                        <input id="config-catalog-items" type="range" min="4" max="100" step="1" />
                    </div>
                    <div class="field">
                        <label for="config-refresh-interval">Refresh cadence <span class="helper">How often the AI rethinks the catalogs</span></label>
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
                        <button id="prepare-profile" type="button">Generate catalogs</button>
                        <button id="copy-configured-manifest" type="button" class="secondary">Copy configured manifest</button>
                        <button id="copy-default-manifest" type="button" class="secondary">Copy public manifest</button>
                    </div>
                    <div class="status" id="manifest-status"></div>
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
            const catalogItemsSlider = document.getElementById('config-catalog-items');
            const catalogItemsValue = document.getElementById('catalog-items-value');
            const refreshSelect = document.getElementById('config-refresh-interval');
            const cacheSelect = document.getElementById('config-cache-ttl');
            const prepareProfileButton = document.getElementById('prepare-profile');
            const copyDefaultManifest = document.getElementById('copy-default-manifest');
            const copyConfiguredManifest = document.getElementById('copy-configured-manifest');
            const manifestPreview = document.getElementById('manifest-preview');
            const copyMessage = document.getElementById('copy-message');
            const manifestStatus = document.getElementById('manifest-status');
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
            let preparePending = false;
            let profileStatus = null;
            let statusPollTimer = null;

            openrouterModel.value = defaults.openrouterModel || '';
            catalogSlider.value = defaults.catalogCount || catalogSlider.min || 1;
            catalogValue.textContent = catalogSlider.value;
            const defaultCatalogItems = defaults.catalogItemCount || catalogItemsSlider.min || 4;
            catalogItemsSlider.value = defaultCatalogItems;
            catalogItemsValue.textContent = catalogItemsSlider.value;
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
            updateManifestUi();
            updateManifestStatus();
            void fetchProfileStatus({ useConfig: true, silent: true }).then((status) => {
                if (status && status.refreshing) {
                    scheduleStatusPoll();
                }
            });

            catalogSlider.addEventListener('input', () => {
                catalogValue.textContent = catalogSlider.value;
                markProfileDirty();
                updateManifestPreview();
            });
            catalogItemsSlider.addEventListener('input', () => {
                catalogItemsValue.textContent = catalogItemsSlider.value;
                markProfileDirty();
                updateManifestPreview();
            });
            openrouterModel.addEventListener('input', () => {
                markProfileDirty();
                updateManifestPreview();
            });
            openrouterKey.addEventListener('input', () => {
                markProfileDirty();
                updateManifestPreview();
            });
            refreshSelect.addEventListener('change', () => {
                ensureOption(refreshSelect, refreshSelect.value, formatSeconds(Number(refreshSelect.value)));
                markProfileDirty();
                updateManifestPreview();
            });
            cacheSelect.addEventListener('change', () => {
                ensureOption(cacheSelect, cacheSelect.value, formatSeconds(Number(cacheSelect.value)));
                markProfileDirty();
                updateManifestPreview();
            });

            copyDefaultManifest.addEventListener('click', async () => {
                await copyToClipboard(baseManifestUrl);
                setCopyMessage('Base manifest URL copied to clipboard.');
            });

            copyConfiguredManifest.addEventListener('click', async () => {
                if (!isProfileReady()) {
                    setCopyMessage('Generate catalogs before copying your manifest URL.');
                    return;
                }
                const url = buildConfiguredUrl();
                await copyToClipboard(url);
                setCopyMessage('Configured manifest URL copied. Install it in Stremio to use your settings.');
            });

            prepareProfileButton.addEventListener('click', () => {
                if (preparePending) {
                    return;
                }
                startProfilePreparation();
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
                stopStatusPolling();
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

            function setManifestStatus(message, variant) {
                manifestStatus.textContent = message || '';
                if (variant === 'success') {
                    manifestStatus.classList.add('success');
                    manifestStatus.classList.remove('error');
                } else if (variant === 'error') {
                    manifestStatus.classList.add('error');
                    manifestStatus.classList.remove('success');
                } else {
                    manifestStatus.classList.remove('success');
                    manifestStatus.classList.remove('error');
                }
            }

            function updateManifestStatus() {
                if (preparePending) {
                    setManifestStatus('Generating catalogs… this typically takes under a minute.');
                    return;
                }
                if (!profileStatus) {
                    setManifestStatus('Adjust the settings and click “Generate catalogs” to warm your manifest URL.');
                    return;
                }
                if (profileStatus.refreshing) {
                    setManifestStatus('Crunching fresh picks in the background… hang tight.');
                    return;
                }
                if (!profileStatus.hasCatalogs) {
                    setManifestStatus('No catalogs available yet—run “Generate catalogs” to get started.', 'error');
                    return;
                }
                let message = 'Catalogs ready to copy.';
                if (profileStatus.lastRefreshedAt) {
                    const refreshedDate = new Date(profileStatus.lastRefreshedAt);
                    if (!Number.isNaN(refreshedDate.getTime())) {
                        message = `Catalogs ready. Last updated ${refreshedDate.toLocaleString()}.`;
                    }
                }
                if (profileStatus.needsRefresh) {
                    message += ' A background refresh is queued.';
                }
                setManifestStatus(message, 'success');
            }

            function isProfileReady() {
                return Boolean(profileStatus && profileStatus.ready && profileStatus.hasCatalogs);
            }

            function stopStatusPolling() {
                if (statusPollTimer) {
                    clearTimeout(statusPollTimer);
                    statusPollTimer = null;
                }
            }

            function markProfileDirty() {
                stopStatusPolling();
                profileStatus = null;
                updateManifestStatus();
                updateManifestUi();
            }

            function updateManifestUi() {
                const traktLocked = traktLoginAvailable && !traktAuth.accessToken;
                prepareProfileButton.disabled = traktLocked || preparePending;
                copyConfiguredManifest.disabled = traktLocked || !isProfileReady();
                manifestLock.classList.toggle('hidden', !traktLoginAvailable || Boolean(traktAuth.accessToken));
            }

            function scheduleStatusPoll(delay = 4000) {
                stopStatusPolling();
                statusPollTimer = setTimeout(async () => {
                    statusPollTimer = null;
                    const status = await fetchProfileStatus({ silent: true });
                    if (status && status.refreshing) {
                        const nextDelay = Math.min(Math.round(delay * 1.25), 15000);
                        scheduleStatusPoll(nextDelay);
                    }
                }, delay);
            }

            function normalizeProfileStatus(raw) {
                if (!raw || typeof raw !== 'object') {
                    return null;
                }
                const profileId = typeof raw.profileId === 'string' ? raw.profileId.trim() : '';
                if (!profileId) {
                    return null;
                }
                return {
                    profileId,
                    hasCatalogs: Boolean(raw.hasCatalogs),
                    needsRefresh: Boolean(raw.needsRefresh),
                    refreshing: Boolean(raw.refreshing),
                    ready: Boolean(raw.ready),
                    lastRefreshedAt: raw.lastRefreshedAt || '',
                    nextRefreshAt: raw.nextRefreshAt || '',
                };
            }

            function collectManifestSettings() {
                return {
                    openrouterKey: openrouterKey.value.trim(),
                    openrouterModel: openrouterModel.value.trim(),
                    catalogCount: catalogSlider.value,
                    catalogItems: catalogItemsSlider.value,
                    refreshInterval: refreshSelect.value,
                    cacheTtl: cacheSelect.value,
                    traktAccessToken: traktAuth.accessToken,
                };
            }

            function buildManifestPayload(options = {}) {
                const { includeProfileId = true } = options;
                const payload = {};
                const settings = collectManifestSettings();
                if (includeProfileId && profileStatus && profileStatus.profileId) {
                    payload.profileId = profileStatus.profileId;
                }
                if (settings.openrouterKey) payload.openrouterKey = settings.openrouterKey;
                if (settings.openrouterModel) payload.openrouterModel = settings.openrouterModel;
                if (settings.catalogCount) payload.catalogCount = Number(settings.catalogCount);
                if (settings.catalogItems) payload.catalogItems = Number(settings.catalogItems);
                if (settings.refreshInterval) payload.refreshInterval = Number(settings.refreshInterval);
                if (settings.cacheTtl) payload.cacheTtl = Number(settings.cacheTtl);
                if (settings.traktAccessToken) payload.traktAccessToken = settings.traktAccessToken;
                return payload;
            }

            function buildProfileQuery(options = {}) {
                const { includeProfileId = true, includeConfig = false } = options;
                const params = new URLSearchParams();
                if (includeProfileId && profileStatus && profileStatus.profileId) {
                    params.set('profileId', profileStatus.profileId);
                }
                if (includeConfig) {
                    const settings = collectManifestSettings();
                    Object.entries(settings).forEach(([key, value]) => {
                        if (value) {
                            params.set(key, value);
                        }
                    });
                }
                const hasKeys = [...params.keys()].length > 0;
                if (!hasKeys && !includeConfig) {
                    return null;
                }
                return params;
            }

            async function startProfilePreparation() {
                preparePending = true;
                stopStatusPolling();
                updateManifestUi();
                updateManifestStatus();
                try {
                    const payload = buildManifestPayload({ includeProfileId: true });
                    payload.waitForCompletion = false;
                    payload.force = true;
                    const response = await fetch('/api/profile/prepare', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(payload),
                    });
                    const data = await response.json().catch(() => ({}));
                    if (!response.ok) {
                        setManifestStatus(resolveErrorMessage(data) || 'Could not start catalog generation.', 'error');
                        return;
                    }
                    const normalized = normalizeProfileStatus(data);
                    if (!normalized) {
                        setManifestStatus('Unexpected response while starting catalog generation.', 'error');
                        return;
                    }
                    profileStatus = normalized;
                    updateManifestPreview();
                    updateManifestUi();
                    updateManifestStatus();
                    if (profileStatus.refreshing) {
                        scheduleStatusPoll();
                    }
                } catch (error) {
                    console.error(error);
                    setManifestStatus('Could not reach the server to generate catalogs.', 'error');
                } finally {
                    preparePending = false;
                    updateManifestUi();
                    updateManifestStatus();
                }
            }

            async function fetchProfileStatus(options = {}) {
                const { silent = false, useConfig = false } = options;
                const params = buildProfileQuery({
                    includeProfileId: !useConfig,
                    includeConfig: useConfig,
                });
                if (!params) {
                    return null;
                }
                try {
                    const response = await fetch(`/api/profile/status?${params.toString()}`);
                    const data = await response.json().catch(() => ({}));
                    if (!response.ok) {
                        if (!silent) {
                            setManifestStatus(resolveErrorMessage(data) || 'Unable to fetch profile status.', 'error');
                        }
                        return null;
                    }
                    const normalized = normalizeProfileStatus(data);
                    if (!normalized) {
                        return null;
                    }
                    profileStatus = normalized;
                    updateManifestPreview();
                    updateManifestUi();
                    updateManifestStatus();
                    return profileStatus;
                } catch (error) {
                    console.error(error);
                    if (!silent) {
                        setManifestStatus('Unable to reach the server for status updates.', 'error');
                    }
                    return null;
                }
            }

            function buildConfiguredUrl() {
                const url = new URL(baseManifestUrl);
                const params = new URLSearchParams();
                const settings = collectManifestSettings();
                if (profileStatus && profileStatus.profileId) {
                    const encodedProfileId = encodeURIComponent(profileStatus.profileId);
                    url.pathname = `/profiles/${encodedProfileId}/manifest.json`;
                } else if (settings.openrouterKey) {
                    params.set('openrouterKey', settings.openrouterKey);
                }
                if (settings.openrouterModel) params.set('openrouterModel', settings.openrouterModel);
                if (settings.catalogCount) params.set('catalogCount', settings.catalogCount);
                if (settings.catalogItems) params.set('catalogItems', settings.catalogItems);
                if (settings.refreshInterval) params.set('refreshInterval', settings.refreshInterval);
                if (settings.cacheTtl) params.set('cacheTtl', settings.cacheTtl);
                if (settings.traktAccessToken) {
                    params.set('traktAccessToken', settings.traktAccessToken);
                }
                url.search = params.toString();
                return url.toString();
            }

            function updateManifestPreview() {
                manifestPreview.textContent = buildConfiguredUrl();
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
                const previousAccess = traktAuth.accessToken;
                const previousRefresh = traktAuth.refreshToken;
                traktAuth.accessToken = access;
                traktAuth.refreshToken = refresh;
                persistTraktTokens(access ? { access_token: access, refresh_token: refresh } : null);
                if (previousAccess !== access || previousRefresh !== refresh) {
                    markProfileDirty();
                }
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
                updateManifestUi();
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
        "catalogItemCount": settings.catalog_item_count,
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
