/**
 * auth.js
 * Handles Client-Side Authentication Logic for Nostr (NIP-07) and SQRL.
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
            window.location.href = '/';
        } else {
            alert("Nostr Login Failed: " + (result.message || "Unknown error"));
        }

    } catch (error) {
        console.error("Nostr Logic Error:", error);
        alert("Authentication canceled or failed.");
    }
}

function loginWithSQRL() {
    const container = document.getElementById('sqrl-qr-container');
    container.classList.remove('hidden');
    container.innerHTML = '<p class="text-warning">SQRL integration pending backend implementation.</p>';
    // TODO: Poll endpoint for SQRL status
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
