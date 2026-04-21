/**
 * auth.js
 * Handles Client-Side Authentication Logic for Nostr (NIP-07 and NIP-46).
 */

async function loginWithNostr() {
    if (!window.nostr) {
        alert("Nostr extension not found! Please install Alby, nos2x, or similar.");
        return;
    }

    try {
        const pubkey = await window.nostr.getPublicKey();
        console.log("Nostr PubKey:", pubkey);

        // NIP-98 / Auth Challenge Construction
        // We create a signed event kind 22242
        const event = {
            kind: 22242,
            created_at: Math.floor(Date.now() / 1000),
            tags: [
                ['challenge', 'login_challenge_placeholder'], // Ideally verified by server nonce
                ['domain', window.location.hostname]
            ],
            content: 'Login to Laguz Tech'
        };

        const signedEvent = await window.nostr.signEvent(event);
        console.log("Signed Event:", signedEvent);

        // Send to Server
        const response = await fetch('/login/nostr', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ event: signedEvent })
        });

        const result = await response.json();
        if (result.success) {
            if (result.vault_locked && result.vault_metadata) {
                console.log("Vault is locked. Attempting decryption...");
                const metadata = result.vault_metadata;

                // NIP-04 Decryption
                // encrypted_dek is a blob from nip04_encrypt(server_priv, user_pub, dek_str)
                try {
                    const decryptedDek = await window.nostr.nip04.decrypt(
                        metadata.sender_pubkey,
                        metadata.encrypted_dek
                    );

                    console.log("DEK decrypted successfully. Sending to server...");

                    // Send Decrypted DEK to server to unlock session
                    const unlockResponse = await fetch('/api/auth/unlock', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ dek: decryptedDek })
                    });

                    const unlockResult = await unlockResponse.json();
                    if (unlockResult.success) {
                        window.location.href = '/';
                    } else {
                        alert("Vault Unlock Failed: " + (unlockResult.message || "Unknown error"));
                    }
                } catch (decryptionError) {
                    console.error("Decryption Error:", decryptionError);
                    alert("Failed to decrypt vault. Please ensure you are using the correct Nostr key and approved the request.");
                }
            } else {
                window.location.href = '/';
            }
        } else {
            alert("Nostr Login Failed: " + (result.message || "Unknown error"));
        }

    } catch (error) {
        console.error("Nostr Logic Error:", error);
        alert("Authentication canceled or failed.");
    }
}



async function registerWithNostr() {
    if (!window.nostr) {
        alert("Nostr extension not found! Please install Alby, nos2x, or similar.");
        return;
    }

    const username = document.getElementById('username')?.value;
    const tradierKey = document.getElementById('tradier_key')?.value;
    const accountId = document.getElementById('account_id')?.value;

    if (!tradierKey || !accountId) {
        alert("Please fill in the Tradier API Key and Account ID first.");
        return;
    }

    try {
        const pubkey = await window.nostr.getPublicKey();

        // Sign Auth Event
        const event = {
            kind: 22242,
            created_at: Math.floor(Date.now() / 1000),
            tags: [
                ['challenge', 'register_challenge'],
                ['domain', window.location.hostname]
            ],
            content: 'Register with Laguz Tech'
        };

        const signedEvent = await window.nostr.signEvent(event);

        // Post to Register Endpoint
        const response = await fetch('/register/nostr', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                username: username || 'NostrUser', // Optional or auto-generated
                tradier_key: tradierKey,
                account_id: accountId,
                event: signedEvent
            })
        });

        const result = await response.json();
        if (result.success) {
            window.location.href = '/';
        } else {
            alert("Nostr Registration Failed: " + (result.message || "Unknown error"));
        }

    } catch (error) {
        console.error("Nostr Registration Error:", error);
        alert("Registration failed: " + error);
    }
}

/**
 * NIP-46 Login Logic
 */
async function loginWithNip46() {
    const statusDiv = document.getElementById('nip46-status');
    const input = document.getElementById('nostr-remote-address').value.trim();

    if (!input) {
        alert("Please enter a NIP-05 address or Bunker URI.");
        return;
    }

    statusDiv.innerText = "Initializing Nostr Connect...";
    statusDiv.style.color = "var(--md-sys-color-primary)";

    try {
        // Ensure NostrTools is loaded
        if (typeof window.NostrTools === 'undefined') {
            throw new Error("NostrTools library not loaded. Check internet connection or ad-blocker.");
        }

        const { Nip46, generateSecretKey, getPublicKey, nip19, SimplePool } = window.NostrTools;

        // Note: old nostr-tools used generatePrivateKey, new uses generateSecretKey. 
        // We try both to be safe or check version. Usually window.NostrTools implies recent bundle.
        const generateSk = generateSecretKey || window.NostrTools.generatePrivateKey;

        let bunkerUri = input;
        
        if (!bunkerUri.startsWith("bunker://")) {
            throw new Error("Please use the full 'bunker://' URI from your signer app.");
        }

        const url = new URL(bunkerUri);
        const remotePubkey = url.pathname.replace('//', '');
        const params = new URLSearchParams(url.search);
        const relays = params.getAll('relay');

        if (relays.length === 0) {
            throw new Error("No relays found in Bunker URI.");
        }

        // 1. Generate local ephemeral key
        const localSk = generateSk();
        // localSk is Uint8Array in new tools, hex string in old. 
        // getPublicKey handles both usually?
        let localPk;
        try {
            localPk = getPublicKey(localSk);
        } catch (e) {
            // Fallback for different version compatibility if needed?
            throw e;
        }

        statusDiv.innerText = "Connecting to remote signer...";

        const pool = new SimplePool();
        const signer = new window.NostrTools.nip46.BunkerSigner(localSk, pool, remotePubkey);

        // 3. Connect
        await signer.connect(relays);

        statusDiv.innerText = "Waiting for approval on device...";

        // 4. Get Public Key (this triggers the "Connect" challenge on the remote device)
        const userPubkey = await signer.getPublicKey();
        console.log("Remote PubKey:", userPubkey);

        statusDiv.innerText = "Authorized! Signing login event...";

        // 5. Sign Auth Event
        const eventTemplate = {
            kind: 22242,
            created_at: Math.floor(Date.now() / 1000),
            tags: [
                ['challenge', 'login_challenge_placeholder'],
                ['domain', window.location.hostname]
            ],
            content: 'Login to Laguz Tech via NIP-46'
        };

        const signedEvent = await signer.signEvent(eventTemplate);

        // 6. Send to Backend
        statusDiv.innerText = "Verifying with server...";

        const response = await fetch('/login/nostr', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ event: signedEvent })
        });

        const result = await response.json();

        if (result.success) {
            statusDiv.innerText = "Login successful!";
            if (result.vault_locked && result.vault_metadata) {
                statusDiv.innerText = "Vault locked. Requesting decryption...";
                const metadata = result.vault_metadata;

                try {
                    // Request decryption from Bunker
                    const decryptedDek = await signer.nip04.decrypt(
                        metadata.sender_pubkey,
                        metadata.encrypted_dek
                    );

                    const unlockResponse = await fetch('/api/auth/unlock', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ dek: decryptedDek })
                    });

                    const unlockResult = await unlockResponse.json();
                    if (unlockResult.success) {
                        window.location.href = '/';
                    } else {
                        throw new Error("Vault unlock failed: " + unlockResult.message);
                    }

                } catch (err) {
                    throw new Error("Decryption failed: " + err.message);
                }
            } else {
                window.location.href = '/';
            }
        } else {
            throw new Error(result.message || "Login failed");
        }

    } catch (error) {
        console.error("NIP-46 Error:", error);
        statusDiv.innerText = "Error: " + error.message;
        statusDiv.style.color = "var(--md-sys-color-error)";
    }
}

/**
 * Generate Login QR Code (nostrconnect:// URI)
 */
async function generateLoginQR() {
    const statusDiv = document.getElementById('nip46-status');
    const qrContainer = document.getElementById('qrcode');
    const btnGenerate = document.getElementById('btn-generate-qr');

    statusDiv.innerText = "Initializing...";
    statusDiv.style.color = "var(--md-sys-color-primary)";
    qrContainer.innerHTML = '';
    btnGenerate.disabled = true;

    try {
        if (typeof window.NostrTools === 'undefined') {
            throw new Error("NostrTools library not loaded.");
        }

        const { generateSecretKey, getPublicKey, SimplePool } = window.NostrTools;
        const generateSk = generateSecretKey || window.NostrTools.generatePrivateKey;

        // 1. Generate local ephemeral key
        const localSk = generateSk();
        let localPk;
        try {
            localPk = getPublicKey(localSk);
        } catch (e) {
            throw e;
        }

        // 2. Construct nostrconnect URI
        const relays = [
            'wss://relay.primal.net',
            'wss://relay.damus.io',
            'wss://nos.lol'
        ];
        const appName = 'LaguzTech';
        const metadata = JSON.stringify({ name: appName });
        
        // Use the format without // as some mobile apps prefer it
        const connectUri = `nostrconnect:${localPk}?relay=${encodeURIComponent(relays[0])}&metadata=${encodeURIComponent(metadata)}`;

        // 3. Render QR Code
        new QRCode(qrContainer, {
            text: connectUri,
            width: 256,
            height: 256,
            colorDark : "#000000",
            colorLight : "#ffffff",
            correctLevel : QRCode.CorrectLevel.M
        });

        statusDiv.innerText = "Scan the QR code with your Nostr app...";

        const pool = new SimplePool();
        const filters = [{
            kinds: [24133],
            '#p': [localPk],
            since: Math.floor(Date.now() / 1000)
        }];

        const sub = pool.subscribeMany(
            relays,
            filters,
            {
                async onevent(event) {
                    statusDiv.innerText = "Connection received! Processing...";
                    const remotePubkey = event.pubkey;

                    try {
                        const signer = new window.NostrTools.nip46.BunkerSigner(localSk, pool, remotePubkey);
                        
                        await signer.connect(relays);
                        
                        await new Promise(r => setTimeout(r, 500));

                        const eventTemplate = {
                            kind: 22242,
                            created_at: Math.floor(Date.now() / 1000),
                            tags: [
                                ['challenge', 'login_challenge_qr'],
                                ['domain', window.location.hostname]
                            ],
                            content: 'Login to Laguz Tech via NIP-46'
                        };

                        // Manually calculate ID if needed for backend nostr-sdk verification
                        if (typeof window.NostrTools.getEventHash === 'function') {
                            eventTemplate.id = window.NostrTools.getEventHash(eventTemplate);
                        }

                        statusDiv.innerText = "Requesting login signature...";

                        const signedEvent = await signer.signEvent(eventTemplate);

                        // Ensure id and sig are present
                        if (!signedEvent.id || !signedEvent.sig) {
                             throw new Error("Signer returned incomplete event (missing id or sig)");
                        }

                        statusDiv.innerText = "Verifying with server...";

                        const response = await fetch('/login/nostr', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ event: signedEvent })
                        });

                        const result = await response.json();

                        if (result.success) {
                            statusDiv.innerText = "Login successful!";
                            if (result.vault_locked && result.vault_metadata) {
                                statusDiv.innerText = "Vault locked. Requesting decryption...";
                                const metadata = result.vault_metadata;

                                const decryptedDek = await signer.nip04.decrypt(
                                    metadata.sender_pubkey,
                                    metadata.encrypted_dek
                                );

                                const unlockResponse = await fetch('/api/auth/unlock', {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify({ dek: decryptedDek })
                                });

                                const unlockResult = await unlockResponse.json();
                                if (unlockResult.success) {
                                    window.location.href = '/';
                                } else {
                                    throw new Error("Vault unlock failed: " + unlockResult.message);
                                }
                            } else {
                                window.location.href = '/';
                            }
                        } else {
                            throw new Error(result.message || "Login failed");
                        }
                    } catch (e) {
                        statusDiv.innerText = "Error: " + e.message;
                        statusDiv.style.color = "var(--md-sys-color-error)";
                    }
                }
            }
        );
    } catch (error) {
        console.error("QR Generate Error:", error);
        statusDiv.innerText = "Error: " + error.message;
        btnGenerate.disabled = false;
    }
}
