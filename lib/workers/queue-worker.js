const { sendWhatsAppText } = require("../meta-cloud")

function toPayloadObject(value) {
	if (!value || typeof value !== "object" || Array.isArray(value)) return {}
	return value
}

function getBodyFromMessage(message) {
	if (typeof message.content_text === "string" && message.content_text.trim().length > 0) {
		return message.content_text
	}
	const payload = toPayloadObject(message.content_payload)
	const fallback = payload.body || payload.text
	return typeof fallback === "string" ? fallback : null
}

function withStatusPayload(contentPayload, key, value) {
	const payload = toPayloadObject(contentPayload)
	return { ...payload, [key]: value }
}

async function processClaimedMessage({ supabase, config, message }) {
	try {
		if (message.provider !== "whatsapp") {
			throw new Error(`Unsupported provider ${message.provider}`)
		}

		const body = getBodyFromMessage(message)
		const sendResult = await sendWhatsAppText({
			accessToken: message.access_token,
			phoneNumberId: message.provider_phone_number_id,
			to: message.recipient_phone,
			body,
			graphApiVersion: config.graphApiVersion,
		})

		const { error } = await supabase
			.from("messages")
			.update({
				queue_status: "completed",
				status: "sent",
				provider_message_id: sendResult.providerMessageId || message.provider_message_id,
				provider_timestamp: new Date().toISOString(),
				content_payload: withStatusPayload(
					message.content_payload,
					"worker_last_send_response",
					sendResult.payload,
				),
			})
			.eq("id", message.id)

		if (error) throw error
		return { id: message.id, status: "sent" }
	} catch (error) {
		const messageText = error instanceof Error ? error.message : String(error)
		await supabase
			.from("messages")
			.update({
				queue_status: "failed",
				status: "failed",
				content_payload: withStatusPayload(message.content_payload, "worker_last_error", {
					message: messageText,
					at: new Date().toISOString(),
				}),
			})
			.eq("id", message.id)

		return { id: message.id, status: "failed", error: messageText }
	}
}

function createQueueWorker({ supabase, config }) {
	let pollTimer = null
	let isRunning = false
	let isProcessing = false

	async function processQueue(reason = "poll") {
		if (!isRunning || isProcessing) return
		isProcessing = true
		try {
			const { data, error } = await supabase.rpc("claim_outbound_messages", {
				p_limit: config.claimBatchSize,
			})
			if (error) throw error
			const rows = Array.isArray(data) ? data : []
			if (rows.length === 0) return

			for (const message of rows) {
				const result = await processClaimedMessage({ supabase, config, message })
				if (result.status === "failed") {
					console.error(`[worker] send failed for ${message.id}`, result.error)
				} else {
					console.info(`[worker] sent ${message.id} (${reason})`)
				}
			}
		} catch (error) {
			console.error("[worker] processQueue error", error)
		} finally {
			isProcessing = false
		}
	}

	async function start() {
		if (isRunning) return
		isRunning = true
		await processQueue("startup")
		pollTimer = setInterval(() => {
			void processQueue("poll")
		}, config.pollIntervalMs)

		const channel = supabase
			.channel("queue-worker-messages")
			.on(
				"postgres_changes",
				{ event: "*", schema: "public", table: "messages" },
				(payload) => {
					const row = payload?.new || payload?.old
					if (!row) return
					if (row.direction === "outbound" && row.queue_status === "queued") {
						void processQueue("realtime")
					}
				},
			)
			.subscribe((status) => {
				if (status === "CHANNEL_ERROR" || status === "TIMED_OUT" || status === "CLOSED") {
					console.warn(
						`[worker] realtime channel status=${status}; polling fallback active`,
					)
				}
			})

		return () => {
			if (pollTimer) clearInterval(pollTimer)
			isRunning = false
			supabase.removeChannel(channel)
		}
	}

	return { start, processQueue }
}

module.exports = { createQueueWorker }
