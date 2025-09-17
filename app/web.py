"""HTML page rendering for the interactive configuration experience."""

from __future__ import annotations

import json
from textwrap import dedent

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
        input[type="number"],
        input[type="password"],
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
        input[type="number"]:focus,
        input[type="password"]:focus,
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
        code.inline {
            font-family: 'JetBrains Mono', 'Fira Code', ui-monospace, SFMono-Regular, monospace;
            background: rgba(15,23,42,0.9);
            padding: 0.25rem 0.45rem;
            border-radius: 8px;
            border: 1px solid rgba(148,163,184,0.2);
        }
        .muted {
            font-size: 0.85rem;
            color: rgba(148,163,184,0.9);
        }
        .status {
            margin-top: 0.75rem;
            font-size: 0.9rem;
            color: rgba(226,232,240,0.92);
        }
        .status.error {
            color: #fca5a5;
        }
        .status.success {
            color: #86efac;
        }
        .token-output {
            margin-top: 1rem;
            display: grid;
            gap: 0.5rem;
        }
        .token-output .token-line {
            display: flex;
            gap: 0.5rem;
            align-items: center;
        }
        .token-output input {
            flex: 1;
            font-family: 'JetBrains Mono', ui-monospace, SFMono-Regular, monospace;
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
        .step-list {
            padding-left: 1.25rem;
            margin-top: 0.35rem;
            margin-bottom: 1rem;
            color: rgba(226,232,240,0.9);
        }
        .step-list li + li {
            margin-top: 0.35rem;
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
            <p>Generate a personalised manifest link, guide users through Trakt device login, and share
            your AI powered addon with the world.</p>
        </header>
        <div class="grid">
            <section class="card">
                <h2>Manifest builder</h2>
                <p class="description">Choose how many AI generated catalogs to expose and copy a ready-to-install
                manifest URL. Empty fields fall back to the server defaults.</p>
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
                <div class="field">
                    <label for="config-trakt-client-id">Trakt client ID <span class="helper">Optional – improves rate limits</span></label>
                    <input id="config-trakt-client-id" type="text" placeholder="public-client-id" spellcheck="false" />
                </div>
                <div class="field">
                    <label for="config-trakt-access-token">Trakt access token <span class="helper">Paste the device token below</span></label>
                    <input id="config-trakt-access-token" type="text" placeholder="long-lived token" spellcheck="false" />
                </div>
                <div class="actions">
                    <button id="copy-default-manifest" type="button">Copy public manifest</button>
                    <button id="copy-configured-manifest" type="button" class="secondary">Copy configured manifest</button>
                </div>
                <p class="muted" id="copy-message"></p>
                <div class="preview" id="manifest-preview"></div>
            </section>
            <section class="card">
                <h2>Trakt device login helper</h2>
                <p class="description">No need to leave the page—launch the device flow to mint a long-lived token for
                your users. The secret never leaves their browser.</p>
                <ol class="step-list">
                    <li>Paste your Trakt <strong>client ID</strong> and <strong>client secret</strong>.</li>
                    <li>Click <em>Start device login</em> and follow the instructions at the verification link.</li>
                    <li>Once approved, copy the access token straight into the manifest builder.</li>
                </ol>
                <div class="field">
                    <label for="trakt-client-secret">Trakt client secret <span class="helper">The one provided in your Trakt app</span></label>
                    <input id="trakt-client-secret" type="password" autocomplete="off" spellcheck="false" />
                </div>
                <div class="actions">
                    <button id="start-trakt-login" type="button">Start device login</button>
                </div>
                <div class="status" id="trakt-status"></div>
                <div id="trakt-code" class="hidden">
                    <p>Visit <a id="trakt-verification" href="https://trakt.tv/activate" target="_blank" rel="noreferrer">trakt.tv/activate</a> and enter code
                    <code class="inline" id="trakt-user-code"></code>.</p>
                    <div class="actions">
                        <button id="copy-user-code" type="button" class="secondary">Copy code</button>
                    </div>
                    <p class="muted" id="trakt-countdown"></p>
                </div>
                <div class="token-output hidden" id="trakt-token-output">
                    <div class="token-line">
                        <input id="trakt-access-value" readonly />
                        <button id="copy-access-token" type="button" class="secondary">Copy access token</button>
                    </div>
                    <div class="token-line">
                        <input id="trakt-refresh-value" readonly placeholder="Refresh token (optional)" />
                        <button id="copy-refresh-token" type="button" class="secondary">Copy refresh token</button>
                    </div>
                </div>
            </section>
        </div>
    </main>
    <script>
    (() => {
        const defaults = __DEFAULTS_JSON__;
        const baseManifestUrl = new URL('/manifest.json', window.location.origin).toString();

        const catalogSlider = document.getElementById('config-catalog-count');
        const catalogValue = document.getElementById('catalog-count-value');
        const openrouterModel = document.getElementById('config-openrouter-model');
        const openrouterKey = document.getElementById('config-openrouter-key');
        const refreshSelect = document.getElementById('config-refresh-interval');
        const cacheSelect = document.getElementById('config-cache-ttl');
        const traktClientId = document.getElementById('config-trakt-client-id');
        const traktAccessToken = document.getElementById('config-trakt-access-token');
        const manifestPreview = document.getElementById('manifest-preview');
        const copyMessage = document.getElementById('copy-message');

        const traktLoginButton = document.getElementById('start-trakt-login');
        const traktStatus = document.getElementById('trakt-status');
        const traktCodeContainer = document.getElementById('trakt-code');
        const traktCountdown = document.getElementById('trakt-countdown');
        const traktCodeValue = document.getElementById('trakt-user-code');
        const traktVerificationLink = document.getElementById('trakt-verification');
        const traktTokenOutput = document.getElementById('trakt-token-output');
        const traktCopyCodeButton = document.getElementById('copy-user-code');
        const traktCopyAccessButton = document.getElementById('copy-access-token');
        const traktCopyRefreshButton = document.getElementById('copy-refresh-token');
        const traktAccessOutput = document.getElementById('trakt-access-value');
        const traktRefreshOutput = document.getElementById('trakt-refresh-value');
        const traktClientSecret = document.getElementById('trakt-client-secret');

        const refreshDefaults = String(defaults.refreshIntervalSeconds || 43200);
        const cacheDefaults = String(defaults.responseCacheSeconds || 1800);

        openrouterModel.value = defaults.openrouterModel || '';
        catalogSlider.value = String(defaults.catalogCount || 6);
        catalogValue.textContent = catalogSlider.value;
        traktClientId.value = defaults.traktClientId || '';
        traktAccessToken.value = defaults.traktAccessToken || '';

        ensureOption(refreshSelect, refreshDefaults, formatSeconds(Number(refreshDefaults)));
        ensureOption(cacheSelect, cacheDefaults, formatSeconds(Number(cacheDefaults)));
        refreshSelect.value = refreshDefaults;
        cacheSelect.value = cacheDefaults;

        updateManifestPreview();

        catalogSlider.addEventListener('input', () => {
            catalogValue.textContent = catalogSlider.value;
            updateManifestPreview();
        });

        [openrouterModel, openrouterKey, refreshSelect, cacheSelect, traktClientId, traktAccessToken].forEach((el) => {
            el.addEventListener('input', updateManifestPreview);
            el.addEventListener('change', updateManifestPreview);
        });

        document.getElementById('copy-default-manifest').addEventListener('click', async () => {
            await copyToClipboard(baseManifestUrl);
            setCopyMessage('Base manifest URL copied to clipboard.');
        });

        document.getElementById('copy-configured-manifest').addEventListener('click', async () => {
            const url = buildConfiguredUrl();
            await copyToClipboard(url);
            setCopyMessage('Configured manifest URL copied. Install it in Stremio to use your settings.');
        });

        traktCopyCodeButton.addEventListener('click', () => copyToClipboard(traktCodeValue.textContent));
        traktCopyAccessButton.addEventListener('click', () => copyToClipboard(traktAccessOutput.value));
        traktCopyRefreshButton.addEventListener('click', () => {
            if (traktRefreshOutput.value) {
                copyToClipboard(traktRefreshOutput.value);
            }
        });

        let copyTimeout = null;
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
            const traktId = traktClientId.value.trim();
            if (traktId) params.set('traktClientId', traktId);
            const traktToken = traktAccessToken.value.trim();
            if (traktToken) params.set('traktAccessToken', traktToken);
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

        function showTraktStatus(message, isError = false, isSuccess = false) {
            traktStatus.textContent = message;
            traktStatus.classList.toggle('error', isError);
            traktStatus.classList.toggle('success', isSuccess);
            if (!message) {
                traktStatus.classList.remove('error');
                traktStatus.classList.remove('success');
            }
        }

        const traktState = { pollTimer: null, pollInterval: 5000, expiresAt: null, deviceCode: null };
        let countdownTimer = null;

        traktLoginButton.addEventListener('click', async () => {
            const clientId = traktClientId.value.trim();
            const clientSecret = traktClientSecret.value.trim();
            if (!clientId || !clientSecret) {
                showTraktStatus('Provide your Trakt client ID and client secret first.', true);
                return;
            }

            resetTraktState();
            traktLoginButton.disabled = true;
            showTraktStatus('Requesting a device code from Trakt…');

            try {
                const response = await fetch('/api/trakt/device-code', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ client_id: clientId })
                });
                const payload = await response.json();
                if (!response.ok) {
                    showTraktStatus(resolveErrorMessage(payload), true);
                    traktLoginButton.disabled = false;
                    return;
                }
                traktState.deviceCode = payload.device_code;
                traktState.pollInterval = ((payload.interval || 5) * 1000);
                traktState.expiresAt = Date.now() + ((payload.expires_in || 600) * 1000);

                traktCodeValue.textContent = payload.user_code || 'Unknown';
                traktVerificationLink.href = payload.verification_url || 'https://trakt.tv/activate';
                traktCodeContainer.classList.remove('hidden');
                traktTokenOutput.classList.add('hidden');
                startCountdown();
                showTraktStatus('Enter the code on Trakt and approve access.');
                schedulePoll(clientId, clientSecret);
            } catch (error) {
                console.error(error);
                showTraktStatus('Could not reach the Trakt API. Check your connection and try again.', true);
                traktLoginButton.disabled = false;
            }
        });

        function resetTraktState() {
            if (traktState.pollTimer) {
                clearTimeout(traktState.pollTimer);
                traktState.pollTimer = null;
            }
            if (countdownTimer) {
                clearInterval(countdownTimer);
                countdownTimer = null;
            }
            traktState.deviceCode = null;
            traktState.expiresAt = null;
            traktState.pollInterval = 5000;
            traktCountdown.textContent = '';
            traktCodeContainer.classList.add('hidden');
            traktTokenOutput.classList.add('hidden');
        }

        function startCountdown() {
            if (!traktState.expiresAt) return;
            updateCountdown();
            countdownTimer = setInterval(updateCountdown, 1000);
        }

        function updateCountdown() {
            if (!traktState.expiresAt) return;
            const remainingMs = traktState.expiresAt - Date.now();
            if (remainingMs <= 0) {
                traktCountdown.textContent = 'Device code expired. Start again for a new code.';
                if (traktState.pollTimer) {
                    clearTimeout(traktState.pollTimer);
                    traktState.pollTimer = null;
                }
                traktLoginButton.disabled = false;
                if (countdownTimer) {
                    clearInterval(countdownTimer);
                    countdownTimer = null;
                }
                return;
            }
            const totalSeconds = Math.floor(remainingMs / 1000);
            const minutes = Math.floor(totalSeconds / 60);
            const seconds = totalSeconds % 60;
            traktCountdown.textContent = `Code expires in ${minutes}m ${String(seconds).padStart(2, '0')}s`;
        }

        function schedulePoll(clientId, clientSecret) {
            if (traktState.pollTimer) {
                clearTimeout(traktState.pollTimer);
            }
            traktState.pollTimer = setTimeout(() => pollForToken(clientId, clientSecret), traktState.pollInterval);
        }

        async function pollForToken(clientId, clientSecret) {
            if (!traktState.deviceCode) {
                traktLoginButton.disabled = false;
                return;
            }
            try {
                const response = await fetch('/api/trakt/device-token', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        client_id: clientId,
                        client_secret: clientSecret,
                        device_code: traktState.deviceCode
                    })
                });
                const payload = await response.json();
                if (response.ok) {
                    showTraktStatus('Trakt authorised successfully! Tokens ready below.', false, true);
                    traktLoginButton.disabled = false;
                    if (traktState.pollTimer) {
                        clearTimeout(traktState.pollTimer);
                        traktState.pollTimer = null;
                    }
                    if (countdownTimer) {
                        clearInterval(countdownTimer);
                        countdownTimer = null;
                    }
                    traktCodeContainer.classList.add('hidden');
                    traktTokenOutput.classList.remove('hidden');
                    traktAccessOutput.value = payload.access_token || '';
                    traktRefreshOutput.value = payload.refresh_token || '';
                    if (payload.access_token) {
                        traktAccessToken.value = payload.access_token;
                        updateManifestPreview();
                    }
                    return;
                }

                const errorCode = extractErrorCode(payload);
                if (errorCode === 'authorization_pending') {
                    showTraktStatus('Waiting for you to approve the device on Trakt…');
                    schedulePoll(clientId, clientSecret);
                    return;
                }
                if (errorCode === 'slow_down') {
                    traktState.pollInterval += 5000;
                    showTraktStatus('Trakt asked us to slow down—retrying shortly…');
                    schedulePoll(clientId, clientSecret);
                    return;
                }
                if (errorCode === 'expired_token') {
                    showTraktStatus('Device code expired. Start again to mint a new token.', true);
                } else {
                    showTraktStatus(resolveErrorMessage(payload), true);
                }
                traktLoginButton.disabled = false;
                if (traktState.pollTimer) {
                    clearTimeout(traktState.pollTimer);
                    traktState.pollTimer = null;
                }
            } catch (error) {
                console.error(error);
                showTraktStatus('Network error while contacting Trakt.', true);
                traktLoginButton.disabled = false;
                if (traktState.pollTimer) {
                    clearTimeout(traktState.pollTimer);
                    traktState.pollTimer = null;
                }
            }
        }

        function extractErrorCode(payload) {
            if (!payload) return null;
            if (typeof payload.detail === 'string') return payload.detail;
            if (payload.detail && typeof payload.detail === 'object') {
                return payload.detail.error || payload.detail.code || null;
            }
            return payload.error || payload.error_code || null;
        }

        function resolveErrorMessage(payload) {
            if (!payload) {
                return 'Unexpected error communicating with Trakt.';
            }
            if (typeof payload.detail === 'string') {
                return payload.detail;
            }
            if (payload.detail && typeof payload.detail === 'object') {
                return payload.detail.description || payload.detail.error || 'Trakt rejected the request.';
            }
            return payload.message || payload.error_description || payload.error || 'Trakt rejected the request.';
        }
    })();
    </script>
</body>
</html>
    """
)


def render_config_page(settings: Settings) -> str:
    """Return the full HTML for the `/config` landing page."""

    defaults = {
        "appName": settings.app_name,
        "openrouterModel": settings.openrouter_model,
        "catalogCount": settings.catalog_count,
        "refreshIntervalSeconds": settings.refresh_interval_seconds,
        "responseCacheSeconds": settings.response_cache_seconds,
        "traktClientId": settings.trakt_client_id or "",
        "traktAccessToken": settings.trakt_access_token or "",
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
