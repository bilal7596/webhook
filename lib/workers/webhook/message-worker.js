const { processStatusEvent, toStatusEventFromJobPayload } = require("../../status-handler")

function asObject(value) {
	return value && typeof value === "object" && !Array.isArray(value) ? value : {}
}

async function handleStatusUpdate({ supabase, job }) {
	const event = toStatusEventFromJobPayload(job.payload)
	if (!event.status) {
		throw new Error("status_update job missing status payload.")
	}

	return processStatusEvent({
		supabase,
		event,
		socialAccountId: job.social_account_id || null,
	})
}

async function handleInboundMessage({ supabase, job }) {
	const payload = asObject(job.payload)
	const message = asObject(payload.message)
	const from = message.from || "unknown"
	const messageId = message.id || "unknown"
	await supabase
		.from("webhook_events")
		.update({
			processing_state: "processing",
		})
		.eq("id", job.event_id)
	return {
		status: "processed",
		note: `inbound message observed from ${from} (${messageId})`,
	}
}

async function handleDefault({ job }) {
	return {
		status: "processed",
		note: `message lane no-op for ${job.job_type}`,
	}
}

function createMessageHandlers() {
	return {
		status_update: handleStatusUpdate,
		message_text: handleInboundMessage,
		message_image: handleInboundMessage,
		message_audio: handleInboundMessage,
		message_video: handleInboundMessage,
		message_document: handleInboundMessage,
		message_interactive: handleInboundMessage,
		default: handleDefault,
	}
}

module.exports = { createMessageHandlers }
