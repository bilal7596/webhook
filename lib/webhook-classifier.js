function detectProvider(payload) {
	const objectValue = String(payload?.object || "").toLowerCase()
	if (objectValue.includes("instagram")) return "instagram"
	return "whatsapp"
}

function asArray(value) {
	return Array.isArray(value) ? value : []
}

function asObject(value) {
	return value && typeof value === "object" && !Array.isArray(value) ? value : {}
}

function messageJobType(message) {
	return message?.type ? `message_${message.type}` : "message_text"
}

function classifyWebhookPayload(args) {
	const {
		payload,
		eventContext,
		defaultMaxAttempts = 5,
		appMaxAttempts = 10,
	} = args
	const provider = detectProvider(payload)
	const eventPayload = asObject(payload)
	const entries = asArray(eventPayload.entry)
	const jobs = []

	entries.forEach((entry, entryIndex) => {
		const entryData = asObject(entry)
		const changes = asArray(entryData.changes)
		changes.forEach((change, changeIndex) => {
			const changeData = asObject(change)
			const value = asObject(changeData.value)
			const field = typeof changeData.field === "string" ? changeData.field : "unknown"

			if (field === "messages") {
				const messages = asArray(value.messages)
				const statuses = asArray(value.statuses)

				messages.forEach((message, messageIndex) => {
					const messageData = asObject(message)
					const externalMessageId =
						typeof messageData.id === "string" ? messageData.id : `m-${messageIndex}`
					jobs.push({
						lane: "message",
						job_type: messageJobType(messageData),
						fragment_key: `${entryIndex}:${changeIndex}:message:${externalMessageId}`,
						payload: {
							field,
							entry: entryData,
							change: changeData,
							value,
							message: messageData,
							message_index: messageIndex,
						},
						provider,
						customer_id: eventContext.customerId,
						social_account_id: eventContext.socialAccountId,
						max_attempts: defaultMaxAttempts,
					})
				})

				statuses.forEach((status, statusIndex) => {
					const statusData = asObject(status)
					const statusMessageId =
						typeof statusData.id === "string" ? statusData.id : `s-${statusIndex}`
					jobs.push({
						lane: "message",
						job_type: "status_update",
						fragment_key: `${entryIndex}:${changeIndex}:status:${statusMessageId}`,
						payload: {
							field,
							entry: entryData,
							change: changeData,
							value,
							status: statusData,
							status_index: statusIndex,
						},
						provider,
						customer_id: eventContext.customerId,
						social_account_id: eventContext.socialAccountId,
						max_attempts: defaultMaxAttempts,
					})
				})
				return
			}

			jobs.push({
				lane: "app",
				job_type: `app_${field}`,
				fragment_key: `${entryIndex}:${changeIndex}:app:${field}`,
				payload: {
					field,
					entry: entryData,
					change: changeData,
					value,
				},
				provider,
				customer_id: eventContext.customerId,
				social_account_id: eventContext.socialAccountId,
				max_attempts: appMaxAttempts,
			})
		})
	})

	if (jobs.length === 0) {
		jobs.push({
			lane: "app",
			job_type: "app_unclassified_payload",
			fragment_key: "root",
			payload: { payload: eventPayload },
			provider,
			customer_id: eventContext.customerId,
			social_account_id: eventContext.socialAccountId,
			max_attempts: appMaxAttempts,
		})
	}

	return jobs
}

module.exports = { classifyWebhookPayload, detectProvider }
