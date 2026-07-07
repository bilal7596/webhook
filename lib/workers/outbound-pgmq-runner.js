const { sendWhatsAppText } = require("../meta-cloud")
const {
	computeRetryDelaySeconds,
	isTerminalFailure,
	nextAttemptPayload,
} = require("../job-retry")

function asObject(value) {
	if (!value || typeof value !== "object" || Array.isArray(value)) return {}
	return value
}

function getBodyFromContext(context) {
	if (typeof context.content_text === "string" && context.content_text.trim().length > 0) {
		return context.content_text
	}
	const payload = asObject(context.content_payload)
	const fallback = payload.body || payload.text
	return typeof fallback === "string" ? fallback : null
}

async function loadSendContext(supabase, messageId) {
	const { data, error } = await supabase.rpc("get_outbound_message_send_context", {
		p_message_id: messageId,
	})
	if (error) throw error
	const rows = Array.isArray(data) ? data : []
	return rows[0] ?? null
}

async function processQueueJob({ supabase, config, job }) {
	const payload = asObject(job.message)
	const messageId = payload.message_id
	if (!messageId) {
		throw new Error("PGMQ job missing message_id")
	}

	const attempt = Math.max(Number(payload.attempt) || 1, 1)
	const maxAttempts = Math.max(
		Number(payload.max_attempts) || config.outboundMaxAttempts || 5,
		1,
	)

	await supabase.rpc("mark_outbound_message_processing", { p_message_id: messageId })

	const context = await loadSendContext(supabase, messageId)
	if (!context) {
		throw new Error(`Outbound message ${messageId} not found`)
	}

	if (context.provider !== "whatsapp") {
		throw new Error(`Unsupported provider ${context.provider}`)
	}

	const body = getBodyFromContext(context)
	if (!body) {
		throw new Error("Outbound message is missing body text")
	}

	try {
		const sendResult = await sendWhatsAppText({
			accessToken: context.access_token,
			phoneNumberId: context.provider_phone_number_id,
			to: context.recipient_phone,
			body,
			graphApiVersion: config.graphApiVersion,
		})

		const { error: completeError } = await supabase.rpc("complete_outbound_message_send", {
			p_message_id: messageId,
			p_provider_message_id: sendResult.providerMessageId || context.provider_message_id,
			p_send_response: sendResult.payload ?? {},
		})
		if (completeError) throw completeError

		const { error: deleteError } = await supabase.rpc("pgmq_delete_outbound_message", {
			p_msg_id: job.msg_id,
		})
		if (deleteError) throw deleteError

		return { messageId, status: "sent" }
	} catch (error) {
		const errorMessage = error instanceof Error ? error.message : String(error)

		if (isTerminalFailure(attempt, maxAttempts)) {
			const { error: archiveError } = await supabase.rpc("pgmq_archive_outbound_message", {
				p_msg_id: job.msg_id,
			})
			if (archiveError) throw archiveError

			const { error: failError } = await supabase.rpc("fail_outbound_message_terminal", {
				p_message_id: messageId,
				p_customer_id: payload.customer_id,
				p_pgmq_msg_id: job.msg_id,
				p_attempt_count: attempt,
				p_error: errorMessage,
				p_payload: payload,
			})
			if (failError) throw failError

			return { messageId, status: "dead", error: errorMessage }
		}

		const retryDelaySeconds = computeRetryDelaySeconds(
			attempt,
			config.webhookRetryDelaySeconds,
		)
		const nextPayload = nextAttemptPayload(payload, errorMessage)

		const { error: deleteError } = await supabase.rpc("pgmq_delete_outbound_message", {
			p_msg_id: job.msg_id,
		})
		if (deleteError) throw deleteError

		const { error: requeueError } = await supabase.rpc("pgmq_send_outbound_message", {
			p_payload: nextPayload,
			p_delay_seconds: retryDelaySeconds,
		})
		if (requeueError) throw requeueError

		const { error: resetError } = await supabase.rpc("requeue_outbound_message", {
			p_message_id: messageId,
		})
		if (resetError) throw resetError

		return { messageId, status: "retry", error: errorMessage, retryDelaySeconds }
	}
}

function createOutboundPgmqRunner({ supabase, config }) {
	let pollTimer = null
	let isRunning = false
	let isProcessing = false

	async function processQueue(reason = "poll") {
		if (!isRunning || isProcessing) return
		isProcessing = true
		try {
			const { data, error } = await supabase.rpc("pgmq_read_outbound_messages", {
				p_vt_seconds: config.webhookLeaseSeconds,
				p_qty: config.claimBatchSize,
			})
			if (error) throw error
			const rows = Array.isArray(data) ? data : []
			if (rows.length === 0) return

			for (const job of rows) {
				try {
					const result = await processQueueJob({ supabase, config, job })
					if (result.status === "sent") {
						console.info(`[outbound-pgmq] sent ${result.messageId} (${reason})`)
					} else if (result.status === "retry") {
						console.warn(
							`[outbound-pgmq] retry ${result.messageId} in ${result.retryDelaySeconds}s (${reason})`,
							result.error,
						)
					} else {
						console.error(
							`[outbound-pgmq] dead ${result.messageId} (${reason})`,
							result.error,
						)
					}
				} catch (jobError) {
					const message = jobError instanceof Error ? jobError.message : String(jobError)
					console.error(`[outbound-pgmq] job ${job.msg_id} failed (${reason})`, message)
				}
			}
		} catch (error) {
			console.error("[outbound-pgmq] processQueue error", error)
		} finally {
			isProcessing = false
		}
	}

	async function start() {
		if (isRunning) return () => {}
		isRunning = true
		await processQueue("startup")
		pollTimer = setInterval(() => {
			void processQueue("poll")
		}, config.pollIntervalMs)

		const channel = supabase
			.channel("outbound-pgmq-messages")
			.on(
				"postgres_changes",
				{ event: "INSERT", schema: "public", table: "messages" },
				(payload) => {
					console.log("new outbound message payload", payload)
					const row = payload?.new
					if (!row) return
					if (row.direction === "outbound" && row.queue_status === "queued") {
						void processQueue("realtime")
					}
				},
			)
			.subscribe((status) => {
				if (status === "CHANNEL_ERROR" || status === "TIMED_OUT" || status === "CLOSED") {
					console.warn(
						`[outbound-pgmq] realtime status=${status}; polling fallback active`,
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

module.exports = { createOutboundPgmqRunner }
