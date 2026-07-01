const PROVIDER_STATUS_ORDER = {
	received: 0,
	sent: 1,
	delivered: 2,
	read: 3,
}

function asArray(value) {
	return Array.isArray(value) ? value : []
}

function asObject(value) {
	return value && typeof value === "object" && !Array.isArray(value) ? value : {}
}

function asString(value) {
	return typeof value === "string" && value.length > 0 ? value : null
}

function shouldApplyStatus(currentStatus, nextStatus) {
	if (!currentStatus) return true
	if (currentStatus === nextStatus) return false
	if (currentStatus === "failed") return false
	if (nextStatus === "failed") return currentStatus !== "read"
	if (currentStatus === "read") return false

	const currentOrder = PROVIDER_STATUS_ORDER[currentStatus]
	const nextOrder = PROVIDER_STATUS_ORDER[nextStatus]
	if (typeof currentOrder !== "number" || typeof nextOrder !== "number") return true
	return nextOrder >= currentOrder
}

function buildStatusTimestampPatch({
	nextStatus,
	eventTimestamp,
	currentSentAt,
	currentDeliveredAt,
	currentReadAt,
}) {
	const patch = {}

	if (nextStatus === "sent") {
		if (!currentSentAt) patch.sent_at = eventTimestamp
		return patch
	}

	if (nextStatus === "delivered") {
		if (!currentSentAt) patch.sent_at = eventTimestamp
		if (!currentDeliveredAt) patch.delivered_at = eventTimestamp
		return patch
	}

	if (nextStatus === "read") {
		if (!currentSentAt) patch.sent_at = eventTimestamp
		if (!currentDeliveredAt) patch.delivered_at = eventTimestamp
		if (!currentReadAt) patch.read_at = eventTimestamp
		return patch
	}

	return patch
}

function extractStatusEvents(payload) {
	const events = []
	const entries = asArray(payload?.entry)

	for (const entry of entries) {
		const entryData = asObject(entry)
		const changes = asArray(entryData?.changes)
		for (const change of changes) {
			const changeData = asObject(change)
			const value = asObject(changeData?.value)
			const metadata = asObject(value?.metadata)
			const statuses = asArray(value?.statuses)
			for (const statusPayload of statuses) {
				const status = statusPayload?.status
				if (!["sent", "delivered", "read", "failed"].includes(status)) continue
				const statusErrors = asArray(statusPayload?.errors)
				const firstError = asObject(statusErrors[0])
				events.push({
					eventKey:
						statusPayload?.id ||
						`${status}-${statusPayload?.timestamp || Date.now().toString()}`,
					externalMessageId: statusPayload?.id || null,
					status,
					provider: "whatsapp",
					providerPhoneNumberId: metadata?.phone_number_id || null,
					providerDisplayPhoneNumber: metadata?.display_phone_number || null,
					providerTimestamp: Number.isFinite(Number(statusPayload?.timestamp))
						? new Date(Number(statusPayload.timestamp) * 1000).toISOString()
						: new Date().toISOString(),
					errorMessage:
						firstError?.title || firstError?.message || firstError?.error_data || null,
					rawPayload: statusPayload,
				})
			}
		}
	}

	return events
}

async function resolveSocialAccountForStatus({ supabase, event, socialAccountId }) {
	if (socialAccountId) {
		const { data, error } = await supabase
			.from("social_accounts")
			.select("id, customer_id")
			.eq("id", socialAccountId)
			.limit(1)
			.maybeSingle()
		if (error) throw error
		return data
	}

	const baseQuery = supabase
		.from("social_accounts")
		.select("id, customer_id")
		.eq("provider", event.provider)
		.limit(1)

	if (event.providerPhoneNumberId) {
		const { data, error } = await baseQuery
			.eq("provider_phone_number_id", event.providerPhoneNumberId)
			.maybeSingle()
		if (error) throw error
		if (data) return data
	}

	if (event.providerDisplayPhoneNumber) {
		const { data, error } = await baseQuery
			.eq("provider_display_phone_number", event.providerDisplayPhoneNumber)
			.maybeSingle()
		if (error) throw error
		if (data) return data
	}

	const { data, error } = await baseQuery.maybeSingle()
	if (error) throw error
	return data
}

async function processStatusEvent({ supabase, event, socialAccountId }) {
	const socialAccount = await resolveSocialAccountForStatus({ supabase, event, socialAccountId })
	if (!socialAccount?.id) {
		return {
			eventKey: event.eventKey,
			status: "skipped",
			reason: "No social account matched status callback.",
		}
	}

	if (!event.externalMessageId) {
		return {
			eventKey: event.eventKey,
			status: "skipped",
			reason: "Status callback missing provider message id.",
		}
	}

	const { data: message, error: messageLookupError } = await supabase
		.from("messages")
		.select("id, status, sent_at, delivered_at, read_at, content_payload")
		.eq("social_account_id", socialAccount.id)
		.eq("provider_message_id", event.externalMessageId)
		.maybeSingle()

	if (messageLookupError) {
		return {
			eventKey: event.eventKey,
			status: "error",
			error: messageLookupError.message,
		}
	}

	if (!message?.id) {
		return {
			eventKey: event.eventKey,
			status: "skipped",
			reason: "No message found for provider callback.",
		}
	}

	if (!shouldApplyStatus(message.status, event.status)) {
		return {
			eventKey: event.eventKey,
			status: "skipped",
			reason: `Ignored out-of-order transition ${message.status || "null"} -> ${event.status}`,
		}
	}

	const contentPayload =
		message.content_payload && typeof message.content_payload === "object"
			? message.content_payload
			: {}

	const { error: updateError } = await supabase
		.from("messages")
		.update({
			status: event.status,
			queue_status: event.status === "failed" ? "failed" : undefined,
			provider_timestamp: event.providerTimestamp,
			...buildStatusTimestampPatch({
				nextStatus: event.status,
				eventTimestamp: event.providerTimestamp,
				currentSentAt: message.sent_at,
				currentDeliveredAt: message.delivered_at,
				currentReadAt: message.read_at,
			}),
			content_payload: {
				...contentPayload,
				status_callback: event.rawPayload,
				status_callback_error: event.errorMessage,
			},
		})
		.eq("id", message.id)

	if (updateError) {
		return {
			eventKey: event.eventKey,
			status: "error",
			error: updateError.message,
		}
	}

	return {
		eventKey: event.eventKey,
		status: "processed",
		messageId: message.id,
		messageStatus: event.status,
	}
}

function toStatusEventFromJobPayload(jobPayload) {
	const payload = asObject(jobPayload)
	const status = asObject(payload.status)
	const value = asObject(payload.value)
	const metadata = asObject(value.metadata)
	const statusErrors = asArray(status.errors)
	const firstError = asObject(statusErrors[0])

	return {
		eventKey: asString(status.id) || `status-${Date.now()}`,
		externalMessageId: asString(status.id),
		status: asString(status.status),
		provider: "whatsapp",
		providerPhoneNumberId: asString(metadata.phone_number_id),
		providerDisplayPhoneNumber: asString(metadata.display_phone_number),
		providerTimestamp: Number.isFinite(Number(status.timestamp))
			? new Date(Number(status.timestamp) * 1000).toISOString()
			: new Date().toISOString(),
		errorMessage: firstError.title || firstError.message || null,
		rawPayload: status,
	}
}

async function processStatusEvents({ supabase, payload }) {
	const events = extractStatusEvents(payload)
	if (events.length === 0) {
		return { processed: 0, results: [] }
	}

	const results = []
	for (const event of events) {
		results.push(await processStatusEvent({ supabase, event, socialAccountId: null }))
	}
	return { processed: results.length, results }
}

module.exports = { processStatusEvents, processStatusEvent, toStatusEventFromJobPayload }
