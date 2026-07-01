const DEFAULT_POLL_INTERVAL_MS = 5000
const DEFAULT_CLAIM_BATCH_SIZE = 10
const DEFAULT_GRAPH_API_VERSION = "v20.0"

function asNumber(value, fallback) {
	const parsed = Number.parseInt(String(value ?? ""), 10)
	return Number.isFinite(parsed) ? parsed : fallback
}

function readConfig() {
	return {
		port: asNumber(process.env.PORT, 3000),
		verifyToken: process.env.META_VERIFY_TOKEN || "",
		metaAppSecret: process.env.META_APP_SECRET || "",
		supabaseUrl: process.env.SUPABASE_URL || "",
		supabaseServiceRoleKey: process.env.SUPABASE_SERVICE_ROLE_KEY || "",
		graphApiVersion: process.env.GRAPH_API_VERSION || DEFAULT_GRAPH_API_VERSION,
		pollIntervalMs: asNumber(process.env.WORKER_POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS),
		claimBatchSize: asNumber(process.env.WORKER_CLAIM_BATCH_SIZE, DEFAULT_CLAIM_BATCH_SIZE),
		enableWorker: process.env.ENABLE_QUEUE_WORKER !== "false",
		enableWebhookMessageWorker: process.env.ENABLE_WEBHOOK_MESSAGE_WORKER !== "false",
		enableWebhookAppWorker: process.env.ENABLE_WEBHOOK_APP_WORKER !== "false",
		webhookLeaseSeconds: asNumber(process.env.WEBHOOK_JOB_LEASE_SECONDS, 120),
		webhookRetryDelaySeconds: asNumber(process.env.WEBHOOK_JOB_RETRY_DELAY_SECONDS, 30),
		webhookReapIntervalMs: asNumber(process.env.WEBHOOK_REAP_INTERVAL_MS, 60000),
	}
}

module.exports = { readConfig }
