const crypto = require("crypto")
const { detectProvider } = require("./webhook-classifier")

function asArray(value) {
	return Array.isArray(value) ? value : []
}

function asObject(value) {
	return value && typeof value === "object" && !Array.isArray(value) ? value : {}
}

function asString(value) {
	return typeof value === "string" && value.length > 0 ? value : null
}

function buildRawDedupeKey(rawBody) {
	const normalized = typeof rawBody === "string" ? rawBody : JSON.stringify(rawBody || {})
	return crypto.createHash("sha256").update(normalized).digest("hex")
}

function collectCandidateIds(payload) {
	const entryIds = new Set()
	const businessIds = new Set()
	const phoneNumberIds = new Set()
	const displayPhones = new Set()
	const entries = asArray(payload?.entry)

	for (const entry of entries) {
		const entryData = asObject(entry)
		const entryId = asString(entryData.id)
		if (entryId) {
			entryIds.add(entryId)
			businessIds.add(entryId)
		}
		const changes = asArray(entryData.changes)
		for (const change of changes) {
			const value = asObject(asObject(change).value)
			const metadata = asObject(value.metadata)
			const wabaInfo = asObject(value.waba_info)
			const wabaId = asString(wabaInfo.waba_id)
			if (wabaId) businessIds.add(wabaId)
			const phoneNumberId = asString(metadata.phone_number_id)
			if (phoneNumberId) phoneNumberIds.add(phoneNumberId)
			const displayPhone = asString(metadata.display_phone_number)
			if (displayPhone) displayPhones.add(displayPhone)
			const phoneNumber = asString(value.phone_number)
			if (phoneNumber) displayPhones.add(phoneNumber)
		}
	}

	return {
		entryIds: [...entryIds],
		businessIds: [...businessIds],
		phoneNumberIds: [...phoneNumberIds],
		displayPhones: [...displayPhones],
	}
}

async function resolveSocialAccountContext({ supabase, payload }) {
	const provider = detectProvider(payload)
	const ids = collectCandidateIds(payload)
	let socialAccount = null

	if (provider === "whatsapp" && ids.phoneNumberIds.length > 0) {
		const { data } = await supabase
			.from("social_accounts")
			.select("id, customer_id, provider")
			.eq("provider", provider)
			.in("provider_phone_number_id", ids.phoneNumberIds)
			.limit(1)
			.maybeSingle()
		socialAccount = data ?? null
	}

	if (!socialAccount && provider === "whatsapp" && ids.displayPhones.length > 0) {
		const { data } = await supabase
			.from("social_accounts")
			.select("id, customer_id, provider")
			.eq("provider", provider)
			.in("provider_display_phone_number", ids.displayPhones)
			.limit(1)
			.maybeSingle()
		socialAccount = data ?? null
	}

	if (!socialAccount && ids.businessIds.length > 0) {
		const { data } = await supabase
			.from("social_accounts")
			.select("id, customer_id, provider")
			.eq("provider", provider)
			.in("provider_business_id", ids.businessIds)
			.limit(1)
			.maybeSingle()
		socialAccount = data ?? null
	}

	if (!socialAccount && ids.entryIds.length > 0) {
		const { data } = await supabase
			.from("social_accounts")
			.select("id, customer_id, provider")
			.eq("provider", provider)
			.in("provider_account_id", ids.entryIds)
			.limit(1)
			.maybeSingle()
		socialAccount = data ?? null
	}

	return {
		provider,
		socialAccountId: socialAccount?.id ?? null,
		customerId: socialAccount?.customer_id ?? null,
	}
}

async function persistRawWebhookEvent({ supabase, payload, headers, rawBody }) {
	const eventContext = await resolveSocialAccountContext({ supabase, payload })
	const eventType = (() => {
		const entry = asObject(asArray(payload?.entry)[0])
		const change = asObject(asArray(entry.changes)[0])
		const field = asString(change.field)
		return field ? `${eventContext.provider}:${field}` : `${eventContext.provider}:raw`
	})()

	const dedupeKey = buildRawDedupeKey(rawBody)
	const { data, error } = await supabase
		.from("webhook_events")
		.insert({
			customer_id: eventContext.customerId,
			social_account_id: eventContext.socialAccountId,
			provider: eventContext.provider,
			event_type: eventType,
			payload: payload || {},
			headers: headers || {},
			dedupe_key: dedupeKey,
			processing_state: "queued",
			queue_enqueued_at: null,
			processing_error: null,
		})
		.select("id, customer_id, social_account_id, provider")
		.single()

	if (error) {
		const duplicateErrorCode = "23505"
		if (error.code === duplicateErrorCode) {
			const { data: existing, error: fetchError } = await supabase
				.from("webhook_events")
				.select("id, customer_id, social_account_id, provider")
				.eq("dedupe_key", dedupeKey)
				.limit(1)
				.maybeSingle()
			if (fetchError) throw fetchError
			if (!existing) throw error
			return {
				eventId: existing.id,
				eventContext: {
					provider: existing.provider,
					customerId: existing.customer_id,
					socialAccountId: existing.social_account_id,
				},
				deduped: true,
			}
		}
		throw error
	}

	return {
		eventId: data.id,
		eventContext: {
			provider: data.provider,
			customerId: data.customer_id,
			socialAccountId: data.social_account_id,
		},
		deduped: false,
	}
}

module.exports = { persistRawWebhookEvent }
