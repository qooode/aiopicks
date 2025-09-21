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
            color-scheme: dark;
            font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif;
            --surface: #141414;
            --surface-muted: #090909;
            --surface-strong: #1f1f1f;
            --text-primary: #f5f5f5;
            --text-muted: #a6a6a6;
            --outline: #1c1c1c;
            --outline-strong: #2b2b2b;
            --accent: #f0f0f0;
            --accent-contrast: #050505;
            background: #000000;
            color: var(--text-primary);
        }
        * {
            box-sizing: border-box;
        }
        body {
            margin: 0;
            min-height: 100vh;
            background: #000000;
        }
        a {
            color: inherit;
            text-decoration: underline;
            text-decoration-color: var(--outline-strong);
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
            color: var(--text-muted);
        }
        .grid {
            display: grid;
            gap: 1.75rem;
        }
        .card {
            background: var(--surface);
            border: 1px solid var(--outline);
            border-radius: 20px;
            padding: 1.75rem;
            box-shadow: 0 24px 40px -28px rgba(0, 0, 0, 0.65);
            backdrop-filter: blur(10px);
        }
        .card h2 {
            margin-top: 0;
            font-size: 1.35rem;
            margin-bottom: 0.75rem;
        }
        .card p.description {
            margin-top: 0;
            color: var(--text-muted);
            margin-bottom: 1.25rem;
        }
        .field {
            margin-bottom: 1.15rem;
        }
        .field label {
            display: flex;
            justify-content: space-between;
            gap: 1rem;
            font-weight: 600;
            font-size: 0.95rem;
            margin-bottom: 0.35rem;
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
            background: var(--surface-muted);
            border: 1px solid var(--outline);
            border-radius: 12px;
            color: var(--text-primary);
            padding: 0.65rem 0.85rem;
            font-size: 1rem;
            transition: border 0.2s ease, box-shadow 0.2s ease, background 0.2s ease;
        }
        input[type="text"]:focus,
        select:focus {
            outline: none;
            border-color: var(--outline-strong);
            box-shadow: 0 0 0 3px rgba(255, 255, 255, 0.1);
            background: var(--surface);
        }
        input[type="range"] {
            width: 100%;
        }
        .range-value {
            font-weight: 600;
            font-size: 0.95rem;
        }
        .field.field-checkbox {
            display: flex;
            flex-direction: column;
            gap: 0.35rem;
        }
        .field.field-checkbox label.checkbox {
            display: inline-flex;
            align-items: center;
            gap: 0.65rem;
            font-weight: 600;
            font-size: 0.95rem;
            color: var(--text-primary);
        }
        .field.field-checkbox label.checkbox input[type="checkbox"] {
            width: 1.1rem;
            height: 1.1rem;
            accent-color: var(--accent);
        }
        .field.field-checkbox p.helper-text {
            margin: 0;
            color: var(--text-muted);
            font-size: 0.85rem;
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
            padding: 0.65rem 1.35rem;
            background: var(--accent);
            color: var(--accent-contrast);
            font-weight: 600;
            font-size: 0.95rem;
            cursor: pointer;
            transition: transform 0.15s ease, box-shadow 0.2s ease, filter 0.2s ease;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 0.6rem;
        }
        button.secondary {
            background: transparent;
            color: var(--text-primary);
            border: 1px solid var(--outline-strong);
        }
        button.loading {
            cursor: progress;
        }
        .spinner {
            display: inline-block;
            width: 1rem;
            height: 1rem;
            border-radius: 999px;
            border: 2px solid currentColor;
            border-right-color: transparent;
            animation: spin 0.8s linear infinite;
            opacity: 0.85;
        }
        button:hover:not(:disabled) {
            transform: translateY(-1px);
            box-shadow: 0 12px 24px -18px rgba(0, 0, 0, 0.65);
        }
        button:disabled {
            cursor: not-allowed;
            opacity: 0.6;
            box-shadow: none;
        }
        .muted {
            font-size: 0.85rem;
            color: var(--text-muted);
        }
        .notice {
            margin-bottom: 1rem;
            padding: 0.75rem 1rem;
            border-radius: 14px;
            background: var(--surface-muted);
            border: 1px dashed var(--outline);
            color: var(--text-primary);
        }
        .status {
            margin-top: 0.85rem;
            font-size: 0.95rem;
            color: var(--text-primary);
            min-height: 1.2em;
            padding: 0.4rem 0.6rem;
            border-radius: 10px;
            background: transparent;
        }
        .status.error {
            background: var(--surface-muted);
            border-left: 4px solid var(--outline-strong);
        }
        .status.success {
            background: var(--surface-muted);
            border-left: 4px solid var(--outline);
        }
        .stats-block {
            margin-top: 1.25rem;
            padding: 1rem 1.25rem;
            border-radius: 16px;
            background: var(--surface-muted);
            border: 1px solid var(--outline);
        }
        .stats-grid {
            display: flex;
            gap: 1.5rem;
            flex-wrap: wrap;
        }
        .stats-item {
            min-width: 120px;
        }
        .stats-number {
            font-size: 1.4rem;
            font-weight: 700;
            color: var(--text-primary);
        }
        .stats-label {
            display: block;
            margin-top: 0.2rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-size: 0.75rem;
            color: var(--text-muted);
        }
        .stats-summary,
        .stats-updated {
            margin: 0.6rem 0 0;
            font-size: 0.85rem;
            color: var(--text-muted);
        }
        .preview {
            background: var(--surface-muted);
            border: 1px dashed var(--outline);
            border-radius: 14px;
            padding: 0.9rem 1rem;
            margin-top: 1rem;
            font-family: 'JetBrains Mono', ui-monospace, SFMono-Regular, monospace;
            font-size: 0.9rem;
            word-break: break-all;
            color: var(--text-primary);
        }
        .pill {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            background: var(--surface-muted);
            border: 1px solid var(--outline);
            border-radius: 999px;
            padding: 0.35rem 0.75rem;
            font-size: 0.85rem;
            font-weight: 600;
            color: var(--text-muted);
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }
        .hidden {
            display: none !important;
        }
        @keyframes spin {
            0% {
                transform: rotate(0deg);
            }
            100% {
                transform: rotate(360deg);
            }
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
            <p class="pill">Stremio Add-on</p>
            <h1>Configure __APP_NAME__</h1>
            <p>Connect your Trakt account, dial in the catalog cadence, and copy an install-ready link for Stremio.</p>
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
                <div class="stats-block hidden" id="trakt-stats">
                    <div class="stats-grid">
                        <div class="stats-item">
                            <div class="stats-number" id="trakt-stats-movies">0</div>
                            <span class="stats-label">Movies watched</span>
                        </div>
                        <div class="stats-item">
                            <div class="stats-number" id="trakt-stats-shows">0</div>
                            <span class="stats-label">Shows watched</span>
                        </div>
                    </div>
                    <p class="stats-summary" id="trakt-stats-summary"></p>
                    <p class="stats-updated" id="trakt-stats-updated"></p>
                </div>
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
                    <label for="config-metadata-addon">Metadata add-on URL <span class="helper">Optional – used to fetch posters and IDs</span></label>
                    <input id="config-metadata-addon" type="text" placeholder="https://example-addon.strem.fun" inputmode="url" spellcheck="false" />
                </div>
                <div class="field field-checkbox">
                    <label class="checkbox" for="config-combine-for-you">
                        <input id="config-combine-for-you" type="checkbox" />
                        <span>Blend “For You” rows</span>
                    </label>
                    <p class="helper-text">Merge the opening movie and series lanes into one alternating feed.</p>
                </div>
                <div class="field">
                    <label for="config-catalog-items">Items per catalog <span class="range-value" id="catalog-items-value"></span></label>
                    <input id="config-catalog-items" type="range" min="4" max="100" step="1" />
                </div>
                <div class="field">
                    <label for="config-generation-retries">Retry budget <span class="helper">Extra AI passes if repeats slip in</span> <span class="range-value" id="generation-retries-value"></span></label>
                    <input id="config-generation-retries" type="range" min="0" max="10" step="1" />
                </div>
                <div class="field">
                    <label for="config-history-limit">History depth <span class="helper">How many recent plays to filter duplicates</span> <span class="range-value" id="history-limit-value"></span></label>
                    <input id="config-history-limit" type="range" min="100" max="10000" step="50" />
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
                    <label for="config-cache-ttl">Response cache <span class="helper">How long Stremio reuses the last response before refetching (AI refresh uses the cadence above)</span></label>
                    <select id="config-cache-ttl">
                        <option value="300">5 minutes</option>
                        <option value="900">15 minutes</option>
                        <option value="1800">30 minutes</option>
                        <option value="3600">60 minutes</option>
                    </select>
                </div>
                <div class="actions">
                    <button id="prepare-profile" type="button">
                        <span class="spinner hidden" id="prepare-spinner" aria-hidden="true"></span>
                        <span class="label">Generate catalogs</span>
                    </button>
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
            const numberFormatter = new Intl.NumberFormat();
            const openrouterKey = document.getElementById('config-openrouter-key');
            const openrouterModel = document.getElementById('config-openrouter-model');
            const metadataAddonInput = document.getElementById('config-metadata-addon');
            const combineForYouToggle = document.getElementById('config-combine-for-you');
            const catalogItemsSlider = document.getElementById('config-catalog-items');
            const catalogItemsValue = document.getElementById('catalog-items-value');
            const generationRetriesSlider = document.getElementById('config-generation-retries');
            const generationRetriesValue = document.getElementById('generation-retries-value');
            const historySlider = document.getElementById('config-history-limit');
            const historyValue = document.getElementById('history-limit-value');
            const refreshSelect = document.getElementById('config-refresh-interval');
            const cacheSelect = document.getElementById('config-cache-ttl');
            const prepareProfileButton = document.getElementById('prepare-profile');
            const prepareSpinner = document.getElementById('prepare-spinner');
            const prepareLabel = prepareProfileButton.querySelector('.label');
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
            const traktStatsBlock = document.getElementById('trakt-stats');
            const traktStatsMovies = document.getElementById('trakt-stats-movies');
            const traktStatsShows = document.getElementById('trakt-stats-shows');
            const traktStatsSummary = document.getElementById('trakt-stats-summary');
            const traktStatsUpdated = document.getElementById('trakt-stats-updated');
            const traktLoginAvailable = Boolean(defaults.traktLoginAvailable);
            const traktCallbackOrigin = (defaults.traktCallbackOrigin || '').trim();
            const traktOrigins = [window.location.origin];
            if (traktCallbackOrigin && !traktOrigins.includes(traktCallbackOrigin)) {
                traktOrigins.push(traktCallbackOrigin);
            }

            const traktAuth = { accessToken: '', refreshToken: '' };
            let traktPending = false;
            let copyTimeout = null;
            let historyLimitTouched = false;
            let generationRetriesTouched = false;
            let combineForYouTouched = false;
            let preparePending = false;
            let profileStatus = null;
            let statusPollTimer = null;

            const profileStorageKey = 'aiopicks.profileId';
            let persistedProfileId = readStoredProfileId();

            function readStoredProfileId() {
                if (typeof window === 'undefined' || !window.localStorage) {
                    return '';
                }
                try {
                    const raw = window.localStorage.getItem(profileStorageKey);
                    if (typeof raw !== 'string') {
                        return '';
                    }
                    const trimmed = raw.trim().toLowerCase();
                    if (!trimmed) {
                        return '';
                    }
                    if (!/^[a-z0-9][a-z0-9-]{4,}$/.test(trimmed)) {
                        return '';
                    }
                    return trimmed;
                } catch (error) {
                    return '';
                }
            }

            function persistProfileId(value) {
                if (typeof value !== 'string') {
                    return;
                }
                const trimmed = value.trim().toLowerCase();
                if (!trimmed) {
                    return;
                }
                persistedProfileId = trimmed;
                if (typeof window === 'undefined' || !window.localStorage) {
                    return;
                }
                try {
                    window.localStorage.setItem(profileStorageKey, trimmed);
                } catch (error) {
                    // Ignore storage failures (e.g. privacy mode or disabled storage).
                }
            }

            function generateProfileId() {
                const prefix = 'user-';
                const bytes = new Uint8Array(6);
                if (window.crypto && window.crypto.getRandomValues) {
                    window.crypto.getRandomValues(bytes);
                } else {
                    for (let index = 0; index < bytes.length; index += 1) {
                        bytes[index] = Math.floor(Math.random() * 256);
                    }
                }
                const suffix = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
                return `${prefix}${suffix}`;
            }

            function resolveProfileId(options = {}) {
                const { createIfMissing = false } = options;
                if (profileStatus && profileStatus.profileId) {
                    if (profileStatus.profileId !== persistedProfileId) {
                        persistProfileId(profileStatus.profileId);
                    }
                    return profileStatus.profileId;
                }
                if (persistedProfileId) {
                    return persistedProfileId;
                }
                if (!createIfMissing) {
                    return '';
                }
                const generated = generateProfileId();
                persistProfileId(generated);
                return generated;
            }

            openrouterModel.value = defaults.openrouterModel || '';
            metadataAddonInput.value = defaults.metadataAddon || '';
            if (combineForYouToggle) {
                combineForYouToggle.checked = Boolean(defaults.combineForYou);
            }
            const defaultCatalogItems = defaults.catalogItemCount || catalogItemsSlider.min || 4;
            catalogItemsSlider.value = defaultCatalogItems;
            catalogItemsValue.textContent = catalogItemsSlider.value;
            const defaultRetryLimit =
                typeof defaults.generationRetryLimit === 'number'
                    ? defaults.generationRetryLimit
                    : Number(generationRetriesSlider.value || 3);
            generationRetriesSlider.value = String(defaultRetryLimit);
            generationRetriesValue.textContent = generationRetriesSlider.value;
            const resolvedHistoryLimit =
                defaults.traktHistoryLimit || historySlider.value || historySlider.max || 1000;
            historySlider.value = resolvedHistoryLimit;
            historyValue.textContent = formatHistoryLimit(resolvedHistoryLimit);
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
            updateTraktStats();
            void fetchProfileStatus({ useConfig: true, silent: true }).then((status) => {
                if (status && status.refreshing) {
                    scheduleStatusPoll();
                }
            });

            catalogItemsSlider.addEventListener('input', () => {
                catalogItemsValue.textContent = catalogItemsSlider.value;
                markProfileDirty();
                updateManifestPreview();
            });
            generationRetriesSlider.addEventListener('input', () => {
                generationRetriesTouched = true;
                generationRetriesValue.textContent = generationRetriesSlider.value;
                markProfileDirty();
                updateManifestPreview();
            });
            historySlider.addEventListener('input', () => {
                historyLimitTouched = true;
                historyValue.textContent = formatHistoryLimit(historySlider.value);
                markProfileDirty();
                updateManifestPreview();
                updateTraktStats();
            });
            openrouterModel.addEventListener('input', () => {
                markProfileDirty();
                updateManifestPreview();
            });
            metadataAddonInput.addEventListener('input', () => {
                markProfileDirty();
                updateManifestPreview();
            });
            if (combineForYouToggle) {
                combineForYouToggle.addEventListener('change', () => {
                    combineForYouTouched = true;
                    markProfileDirty();
                    updateManifestPreview();
                });
            }
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
                    void fetchProfileStatus({ useConfig: true });
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
                updateTraktStats();
            }

            function updateManifestUi() {
                const traktLocked = traktLoginAvailable && !traktAuth.accessToken;
                const generating = preparePending || Boolean(profileStatus && profileStatus.refreshing);
                prepareProfileButton.disabled = traktLocked || generating;
                prepareProfileButton.classList.toggle('loading', generating);
                prepareProfileButton.setAttribute('aria-busy', generating ? 'true' : 'false');
                if (prepareSpinner) {
                    prepareSpinner.classList.toggle('hidden', !generating);
                }
                if (prepareLabel) {
                    prepareLabel.textContent = generating ? 'Generating…' : 'Generate catalogs';
                }
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
                persistProfileId(profileId);
                const historyLimit = Number(raw.traktHistoryLimit);
                const retryLimit = Number(raw.generationRetryLimit);
                const historyRaw = raw.traktHistory && typeof raw.traktHistory === 'object' ? raw.traktHistory : {};
                const normaliseCount = (value) => {
                    const numeric = Number(value);
                    return Number.isFinite(numeric) && numeric >= 0 ? numeric : 0;
                };
                const moviesWatched = normaliseCount(historyRaw.movies);
                const showsWatched = normaliseCount(historyRaw.shows);
                const refreshedAt = typeof historyRaw.refreshedAt === 'string' ? historyRaw.refreshedAt : '';
                const statsRaw = historyRaw.stats && typeof historyRaw.stats === 'object' ? historyRaw.stats : {};
                const movieStatsRaw = statsRaw.movies && typeof statsRaw.movies === 'object' ? statsRaw.movies : {};
                const showStatsRaw = statsRaw.shows && typeof statsRaw.shows === 'object' ? statsRaw.shows : {};
                const episodeStatsRaw = statsRaw.episodes && typeof statsRaw.episodes === 'object' ? statsRaw.episodes : {};
                const totalMinutesRaw = statsRaw.totalMinutes ?? (
                    statsRaw.totals && typeof statsRaw.totals === 'object' ? statsRaw.totals.minutes : undefined
                );
                const stats = {
                    movies: {
                        watched: normaliseCount(movieStatsRaw.watched),
                        plays: normaliseCount(movieStatsRaw.plays),
                        minutes: normaliseCount(movieStatsRaw.minutes),
                    },
                    shows: {
                        watched: normaliseCount(showStatsRaw.watched),
                    },
                    episodes: {
                        watched: normaliseCount(episodeStatsRaw.watched),
                        plays: normaliseCount(episodeStatsRaw.plays),
                        minutes: normaliseCount(episodeStatsRaw.minutes),
                    },
                    totalMinutes: normaliseCount(totalMinutesRaw),
                };
                const hasStats = [
                    stats.movies.watched,
                    stats.movies.plays,
                    stats.movies.minutes,
                    stats.shows.watched,
                    stats.episodes.watched,
                    stats.episodes.plays,
                    stats.episodes.minutes,
                    stats.totalMinutes,
                ].some((value) => Number.isFinite(value) && value > 0);
                const history = {
                    movies: moviesWatched,
                    shows: showsWatched,
                    refreshedAt,
                };
                if (hasStats) {
                    const normalisedStats = {};
                    if (stats.movies.watched || stats.movies.plays || stats.movies.minutes) {
                        normalisedStats.movies = stats.movies;
                    }
                    if (stats.shows.watched) {
                        normalisedStats.shows = stats.shows;
                    }
                    if (stats.episodes.watched || stats.episodes.plays || stats.episodes.minutes) {
                        normalisedStats.episodes = stats.episodes;
                    }
                    if (stats.totalMinutes) {
                        normalisedStats.totalMinutes = stats.totalMinutes;
                    }
                    if (Object.keys(normalisedStats).length > 0) {
                        history.stats = normalisedStats;
                    }
                }
                return {
                    profileId,
                    hasCatalogs: Boolean(raw.hasCatalogs),
                    needsRefresh: Boolean(raw.needsRefresh),
                    refreshing: Boolean(raw.refreshing),
                    ready: Boolean(raw.ready),
                    lastRefreshedAt: raw.lastRefreshedAt || '',
                    nextRefreshAt: raw.nextRefreshAt || '',
                    metadataAddon: typeof raw.metadataAddon === 'string'
                        ? raw.metadataAddon.trim()
                        : '',
                    generationRetryLimit: Number.isFinite(retryLimit) && retryLimit >= 0 ? retryLimit : 0,
                    traktHistoryLimit: Number.isFinite(historyLimit) && historyLimit > 0 ? historyLimit : 0,
                    traktHistory: history,
                    combineForYou: Boolean(raw.combineForYou),
                };
            }

            function collectManifestSettings() {
                return {
                    openrouterKey: openrouterKey.value.trim(),
                    openrouterModel: openrouterModel.value.trim(),
                    metadataAddon: metadataAddonInput.value.trim(),
                    combineForYou: combineForYouToggle ? combineForYouToggle.checked : false,
                    catalogItems: catalogItemsSlider.value,
                    generationRetries: generationRetriesSlider.value,
                    traktHistoryLimit: historySlider.value,
                    refreshInterval: refreshSelect.value,
                    cacheTtl: cacheSelect.value,
                    traktAccessToken: traktAuth.accessToken,
                };
            }

            function buildManifestPayload(options = {}) {
                const { includeProfileId = true } = options;
                const payload = {};
                const settings = collectManifestSettings();
                if (includeProfileId) {
                    const profileId = resolveProfileId({ createIfMissing: true });
                    if (profileId) {
                        payload.profileId = profileId;
                    }
                }
                if (settings.openrouterKey) payload.openrouterKey = settings.openrouterKey;
                if (settings.openrouterModel) payload.openrouterModel = settings.openrouterModel;
                if (settings.catalogItems) payload.catalogItems = Number(settings.catalogItems);
                if (settings.generationRetries !== undefined) {
                    const retries = Number(settings.generationRetries);
                    if (Number.isFinite(retries)) {
                        payload.generationRetries = retries;
                    }
                }
                if (settings.traktHistoryLimit) {
                    payload.traktHistoryLimit = Number(settings.traktHistoryLimit);
                }
                if (settings.refreshInterval) payload.refreshInterval = Number(settings.refreshInterval);
                if (settings.cacheTtl) payload.cacheTtl = Number(settings.cacheTtl);
                if (settings.traktAccessToken) payload.traktAccessToken = settings.traktAccessToken;
                if (settings.metadataAddon) payload.metadataAddon = settings.metadataAddon;
                if (settings.combineForYou) payload.combineForYou = true;
                return payload;
            }

            function buildProfileQuery(options = {}) {
                const { includeProfileId = true, includeConfig = false } = options;
                const params = new URLSearchParams();
                const shouldIncludeProfile = includeProfileId || includeConfig;
                if (shouldIncludeProfile) {
                    const profileId = resolveProfileId({ createIfMissing: includeProfileId });
                    if (profileId) {
                        params.set('profileId', profileId);
                    }
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
                    syncCombineForYouFromStatus();
                    syncHistoryLimitFromStatus();
                    syncGenerationRetriesFromStatus();
                    updateManifestPreview();
                    updateManifestUi();
                    updateManifestStatus();
                    updateTraktStats();
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
                    syncCombineForYouFromStatus();
                    syncHistoryLimitFromStatus();
                    syncGenerationRetriesFromStatus();
                    updateManifestPreview();
                    updateManifestUi();
                    updateManifestStatus();
                    updateTraktStats();
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
                url.search = '';
                const settings = collectManifestSettings();
                const activeProfileId = resolveProfileId({ createIfMissing: false });
                if (activeProfileId) {
                    const encodedProfileId = encodeURIComponent(activeProfileId);
                    url.pathname = `/profiles/${encodedProfileId}/manifest.json`;
                    return url.toString();
                }
                const manifestKeys = [
                    'openrouterKey',
                    'openrouterModel',
                    'metadataAddon',
                    'combineForYou',
                    'catalogItems',
                    'generationRetries',
                    'traktHistoryLimit',
                    'refreshInterval',
                    'cacheTtl',
                    'traktAccessToken',
                ];
                const segments = [];
                manifestKeys.forEach((key) => {
                    const value = settings[key];
                    if (!value) {
                        return;
                    }
                    segments.push(encodeURIComponent(key));
                    segments.push(encodeURIComponent(String(value)));
                });
                if (segments.length > 0) {
                    url.pathname = `/manifest/${segments.join('/')}/manifest.json`;
                } else {
                    url.pathname = '/manifest.json';
                }
                return url.toString();
            }

            function updateManifestPreview() {
                manifestPreview.textContent = buildConfiguredUrl();
            }

            function syncCombineForYouFromStatus() {
                if (!combineForYouToggle || !profileStatus) {
                    return;
                }
                const desired = Boolean(profileStatus.combineForYou);
                if (combineForYouTouched && combineForYouToggle.checked !== desired) {
                    return;
                }
                combineForYouTouched = false;
                combineForYouToggle.checked = desired;
            }

            function syncHistoryLimitFromStatus() {
                if (!profileStatus) {
                    return;
                }
                const limit = Number(profileStatus.traktHistoryLimit);
                if (!Number.isFinite(limit) || limit <= 0) {
                    return;
                }
                const currentValue = Number(historySlider.value);
                if (historyLimitTouched && currentValue !== limit) {
                    return;
                }
                historyLimitTouched = false;
                if (currentValue !== limit) {
                    historySlider.value = String(limit);
                }
                historyValue.textContent = formatHistoryLimit(limit);
            }

            function syncGenerationRetriesFromStatus() {
                if (!profileStatus) {
                    return;
                }
                const limit = Number(profileStatus.generationRetryLimit);
                if (!Number.isFinite(limit) || limit < 0) {
                    return;
                }
                const currentValue = Number(generationRetriesSlider.value);
                if (generationRetriesTouched && currentValue !== limit) {
                    return;
                }
                generationRetriesTouched = false;
                if (currentValue !== limit) {
                    generationRetriesSlider.value = String(limit);
                }
                generationRetriesValue.textContent = generationRetriesSlider.value;
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

            function formatHistoryLimit(value) {
                const numeric = Number(value);
                if (!Number.isFinite(numeric) || numeric <= 0) {
                    return '';
                }
                return `${numberFormatter.format(Math.round(numeric))} plays`;
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

            function formatDurationMinutes(totalMinutes) {
                if (!Number.isFinite(totalMinutes) || totalMinutes <= 0) {
                    return '';
                }
                const rounded = Math.round(totalMinutes);
                const minutesPerDay = 24 * 60;
                const days = Math.floor(rounded / minutesPerDay);
                const hours = Math.floor((rounded % minutesPerDay) / 60);
                const minutes = rounded % 60;
                const parts = [];
                if (days) {
                    parts.push(`${days}d`);
                }
                if (hours) {
                    parts.push(`${hours}h`);
                }
                if (minutes) {
                    parts.push(`${minutes}m`);
                }
                if (parts.length === 0) {
                    return `${rounded}m`;
                }
                return parts.join(' ');
            }

            function updateTraktUi() {
                const connected = Boolean(traktAuth.accessToken);
                traktLoginButton.classList.toggle('hidden', !traktLoginAvailable || connected);
                traktDisconnectButton.classList.toggle('hidden', !connected);
                traktLoginButton.disabled = !traktLoginAvailable || traktPending;
                traktDisconnectButton.disabled = traktPending || !connected;
                updateTraktStats();
            }

            function updateTraktStats() {
                if (!traktStatsBlock || !traktStatsMovies || !traktStatsShows || !traktStatsSummary || !traktStatsUpdated) {
                    return;
                }
                const connected = Boolean(traktAuth.accessToken);
                if (!connected || !profileStatus || !profileStatus.traktHistory) {
                    traktStatsBlock.classList.add('hidden');
                    traktStatsMovies.textContent = '0';
                    traktStatsShows.textContent = '0';
                    traktStatsSummary.textContent = '';
                    traktStatsUpdated.textContent = '';
                    return;
                }
                const history = profileStatus.traktHistory || {};
                const toCount = (value) => {
                    const numeric = Number(value);
                    return Number.isFinite(numeric) && numeric >= 0 ? numeric : 0;
                };
                const movies = toCount(history.movies);
                const shows = toCount(history.shows);
                traktStatsMovies.textContent = numberFormatter.format(movies);
                traktStatsShows.textContent = numberFormatter.format(shows);
                const stats = history.stats && typeof history.stats === 'object' ? history.stats : null;
                const movieStats = stats && typeof stats.movies === 'object' ? stats.movies : {};
                const episodeStats = stats && typeof stats.episodes === 'object' ? stats.episodes : {};
                let totalMinutes = toCount(stats && typeof stats.totalMinutes !== 'undefined' ? stats.totalMinutes : 0);
                const moviePlays = toCount(movieStats && typeof movieStats.plays !== 'undefined' ? movieStats.plays : 0);
                const movieMinutes = toCount(movieStats && typeof movieStats.minutes !== 'undefined' ? movieStats.minutes : 0);
                const episodesWatched = toCount(episodeStats && typeof episodeStats.watched !== 'undefined' ? episodeStats.watched : 0);
                const episodePlays = toCount(episodeStats && typeof episodeStats.plays !== 'undefined' ? episodeStats.plays : 0);
                const episodeMinutes = toCount(episodeStats && typeof episodeStats.minutes !== 'undefined' ? episodeStats.minutes : 0);
                if (!totalMinutes && (movieMinutes || episodeMinutes)) {
                    totalMinutes = movieMinutes + episodeMinutes;
                }
                let summaryLimit = Number(profileStatus.traktHistoryLimit) || 0;
                if (historyLimitTouched) {
                    summaryLimit = Number(historySlider.value) || summaryLimit;
                } else if (!summaryLimit) {
                    summaryLimit = Number(historySlider.value);
                }
                const summaryParts = [];
                if (movies > 0) {
                    let movieLabel = `${numberFormatter.format(movies)} movies`;
                    if (moviePlays > 0 && moviePlays !== movies) {
                        movieLabel += ` (${numberFormatter.format(moviePlays)} plays)`;
                    }
                    summaryParts.push(movieLabel);
                }
                if (shows > 0 && episodesWatched > 0) {
                    let showLabel = `${numberFormatter.format(episodesWatched)} episodes across ${numberFormatter.format(shows)} shows`;
                    if (episodePlays > 0 && episodePlays !== episodesWatched) {
                        showLabel += ` (${numberFormatter.format(episodePlays)} plays)`;
                    }
                    summaryParts.push(showLabel);
                } else if (shows > 0) {
                    summaryParts.push(`${numberFormatter.format(shows)} shows`);
                }
                const summaryMessages = [];
                if (summaryParts.length > 0) {
                    summaryMessages.push(`Lifetime stats: ${summaryParts.join(' · ')}.`);
                }
                if (Number.isFinite(summaryLimit) && summaryLimit > 0) {
                    summaryMessages.push(`Filtering repeats from your latest ${numberFormatter.format(Math.round(summaryLimit))} plays.`);
                }
                traktStatsSummary.textContent = summaryMessages.join(' ');
                const refreshedAt = typeof history.refreshedAt === 'string' ? history.refreshedAt : '';
                const updatedParts = [];
                if (refreshedAt) {
                    const refreshedDate = new Date(refreshedAt);
                    if (!Number.isNaN(refreshedDate.getTime())) {
                        updatedParts.push(`Synced ${refreshedDate.toLocaleString()}.`);
                    }
                }
                const durationText = formatDurationMinutes(totalMinutes);
                if (durationText) {
                    updatedParts.push(`All-time watch time ${durationText}.`);
                }
                traktStatsUpdated.textContent = updatedParts.join(' ');
                traktStatsBlock.classList.remove('hidden');
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
        "manifestName": settings.app_name,
        "openrouterModel": settings.openrouter_model,
        "catalogItemCount": settings.catalog_item_count,
        "generationRetryLimit": settings.generation_retry_limit,
        "traktHistoryLimit": settings.trakt_history_limit,
        "refreshIntervalSeconds": settings.refresh_interval_seconds,
        "responseCacheSeconds": settings.response_cache_seconds,
        "traktAccessToken": settings.trakt_access_token or "",
        "traktLoginAvailable": bool(
            settings.trakt_client_id and settings.trakt_client_secret
        ),
        "traktCallbackOrigin": resolved_callback_origin,
        "metadataAddon": (
            str(settings.metadata_addon_url) if settings.metadata_addon_url else ""
        ),
        "combineForYou": settings.combine_for_you_catalogs,
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
