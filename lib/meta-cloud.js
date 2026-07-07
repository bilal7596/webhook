const crypto = require("crypto")

function verifyMetaSignature(rawBody, signatureHeader, appSecret) {
	if (!appSecret) {
		return {
			ok: true,
			reason: "META_APP_SECRET not configured; skipping signature verification.",
		}
	}
	if (!signatureHeader) {
		return { ok: false, reason: "Missing X-Meta-Signature-256 header." }
	}

	const expectedDigest = crypto
		.createHmac("sha256", appSecret)
		.update(rawBody, "utf8")
		.digest("hex")
	const expected = `sha256=${expectedDigest}`
	const provided = String(signatureHeader)

	const isEqual =
		provided.length === expected.length &&
		crypto.timingSafeEqual(Buffer.from(provided), Buffer.from(expected))

	if (!isEqual) {
		return { ok: false, reason: "Invalid X-Meta-Signature-256 signature." }
	}

	return { ok: true, reason: "ok" }
}

function normalizePhone(value) {
	return String(value || "").replace(/[^\d]/g, "")
}

async function sendWhatsAppText(args) {
	const { accessToken, phoneNumberId, to, body, graphApiVersion } = args

	if (!accessToken) {
		throw new Error("Missing OAuth access token for social account.")
	}
	if (!phoneNumberId) {
		throw new Error("Missing provider phone number id for social account.")
	}
	const recipient = normalizePhone(to)
	if (!recipient) {
		throw new Error("Missing recipient phone number.")
	}
	if (!body || !String(body).trim()) {
		throw new Error("Cannot send empty message body.")
	}

	console.log("phoneNumberId--------", phoneNumberId)
	console.log("accessToken--------", accessToken)

	const response = await fetch(
		`https://graph.facebook.com/${graphApiVersion}/${phoneNumberId}/messages`,
		{
			method: "POST",
			headers: {
				Authorization: `Bearer ${accessToken}`, //EAAONZCmqVEIABR6VlCVUgeEYQyfIkYyb6Ki9Ow7CzxgMFn1GXTJSTGkW957J2cJd53ZBf1teVNnxIslfLF4TOHKa3hzxZBuEm6qGmqZArLyL2WMfyxrRZBRxkdcwSQiO1Xpbc8w0lBQtXD64M9DSiNKyCPIiKhuAk5f6EyozTNE0kPRSLMdjX7B28B6m54Xc5IRnOwMiq4iiJuWCgUKyVCQcol3m20qSV8Kps7GMY
				"Content-Type": "application/json",
			},
			body: JSON.stringify({
				messaging_product: "whatsapp",
				to: recipient,
				type: "text",
				text: { body: String(body) },
			}),
		},
	)

	const payload = await response.json().catch(() => ({}))
	console.log("sendWhatsAppText payload", payload)
	if (!response.ok) {
		const message =
			payload?.error?.message || payload?.error?.error_user_msg || "WhatsApp send failed."
		throw new Error(message)
	}

	const firstMessage = Array.isArray(payload?.messages) ? payload.messages[0] : null
	return {
		providerMessageId: firstMessage?.id || null,
		payload,
	}
}

module.exports = { verifyMetaSignature, sendWhatsAppText }
