function asObject(value) {
	return value && typeof value === "object" && !Array.isArray(value) ? value : {}
}

function asString(value) {
	return typeof value === "string" && value.length > 0 ? value : null
}

async function resolveSocialAccountId({ supabase, job }) {
	if (job.social_account_id) return job.social_account_id
	const payload = asObject(job.payload)
	const value = asObject(payload.value)
	const entry = asObject(payload.entry)
	const metadata = asObject(value.metadata)
	const wabaInfo = asObject(value.waba_info)
	const provider = job.provider || "whatsapp"

	const candidatePhoneNumberId = asString(metadata.phone_number_id)
	if (candidatePhoneNumberId) {
		const { data } = await supabase
			.from("social_accounts")
			.select("id")
			.eq("provider", provider)
			.eq("provider_phone_number_id", candidatePhoneNumberId)
			.limit(1)
			.maybeSingle()
		if (data?.id) return data.id
	}

	const candidateBusinessId = asString(wabaInfo.waba_id) || asString(entry.id)
	if (candidateBusinessId) {
		const { data } = await supabase
			.from("social_accounts")
			.select("id")
			.eq("provider", provider)
			.eq("provider_business_id", candidateBusinessId)
			.limit(1)
			.maybeSingle()
		if (data?.id) return data.id

		const { data: providerAccountData } = await supabase
			.from("social_accounts")
			.select("id")
			.eq("provider", provider)
			.eq("provider_account_id", candidateBusinessId)
			.limit(1)
			.maybeSingle()
		if (providerAccountData?.id) return providerAccountData.id
	}

	return null
}

async function handlePhoneNumberNameUpdate({ supabase, job }) {
	const socialAccountId = await resolveSocialAccountId({ supabase, job })
	if (!socialAccountId) {
		return { status: "skipped", reason: "No social account found for phone name update." }
	}
	const payload = asObject(job.payload)
	const value = asObject(payload.value)
	const displayPhone =
		asString(value.display_phone_number) || asString(value.phone_number) || null
	const { data: accountRow, error: accountLookupError } = await supabase
		.from("social_accounts")
		.select("metadata")
		.eq("id", socialAccountId)
		.limit(1)
		.maybeSingle()
	if (accountLookupError) throw accountLookupError
	const existingMetadata =
		accountRow?.metadata && typeof accountRow.metadata === "object" ? accountRow.metadata : {}

	const { error } = await supabase
		.from("social_accounts")
		.update({
			provider_display_phone_number: displayPhone,
			metadata: { ...existingMetadata, phone_name_update: value },
		})
		.eq("id", socialAccountId)
	if (error) throw error
	return { status: "processed", socialAccountId }
}

async function handleAccountUpdate({ supabase, job }) {
	const socialAccountId = await resolveSocialAccountId({ supabase, job })
	if (!socialAccountId) {
		return { status: "skipped", reason: "No social account found for account update." }
	}

	const payload = asObject(job.payload)
	const value = asObject(payload.value)
	const eventName = asString(value.event) || "UNKNOWN"
	const displayPhone = asString(value.phone_number) || null
	const { data: accountRow, error: accountLookupError } = await supabase
		.from("social_accounts")
		.select("metadata")
		.eq("id", socialAccountId)
		.limit(1)
		.maybeSingle()
	if (accountLookupError) throw accountLookupError
	const existingMetadata =
		accountRow?.metadata && typeof accountRow.metadata === "object" ? accountRow.metadata : {}
	const metadataPatch = { ...existingMetadata, account_update: value }
	const updatePayload = {
		metadata: metadataPatch,
	}

	if (displayPhone) {
		updatePayload.provider_display_phone_number = displayPhone
	}

	if (
		eventName === "PARTNER_REMOVED" ||
		eventName === "ACCOUNT_DELETED" ||
		eventName === "PHONE_NUMBER_REMOVED"
	) {
		updatePayload.status = "disconnected"
	}

	const { error } = await supabase
		.from("social_accounts")
		.update(updatePayload)
		.eq("id", socialAccountId)
	if (error) throw error
	return { status: "processed", socialAccountId, eventName }
}

async function handleDefault({ job }) {
	return { status: "processed", note: `app lane no-op for ${job.job_type}` }
}

function createAppHandlers() {
	return {
		app_phone_number_name_update: handlePhoneNumberNameUpdate,
		app_account_update: handleAccountUpdate,
		default: handleDefault,
	}
}

module.exports = { createAppHandlers }
