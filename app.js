const express = require("express")

const { readConfig } = require("./lib/config")
const { verifyMetaSignature } = require("./lib/meta-cloud")
const { classifyWebhookPayload } = require("./lib/webhook-classifier")
const { persistRawWebhookEvent } = require("./lib/webhook-event-store")
const { createWebhookJobRunner } = require("./lib/webhook-job-runner")
const { createMessageHandlers } = require("./lib/workers/webhook/message-worker")
const { createAppHandlers } = require("./lib/workers/webhook/app-worker")
const { createOutboundPgmqRunner } = require("./lib/workers/outbound-pgmq-runner")
const { createServiceClient } = require("./lib/supabase")

const config = readConfig()
const app = express()
const supabase = createServiceClient(config)

app.use(
	express.json({
		verify: (req, _res, buffer) => {
			req.rawBody = buffer.toString("utf8")
		},
	}),
)

app.get("/", (_req, res) => {
	res.status(200).json({ ok: true, service: "webhook-worker" })
})

app.get("/webhooks/meta", (req, res) => {
	const { "hub.mode": mode, "hub.challenge": challenge, "hub.verify_token": token } = req.query
	if (mode === "subscribe" && token === config.verifyToken) {
		console.log("[webhook] verification succeeded")
		return res.status(200).send(challenge)
	}
	return res.status(403).json({ error: "Webhook verification failed." })
})

app.post("/webhooks/meta", async (req, res) => {
	const signature = verifyMetaSignature(
		req.rawBody || JSON.stringify(req.body || {}),
		req.headers["x-meta-signature-256"] || null,
		config.metaAppSecret,
	)
	if (!signature.ok) {
		return res.status(401).json({ error: signature.reason })
	}

	try {
		const payload = req.body || {}
		const headers = Object.fromEntries(Object.entries(req.headers))
		const rawBody = req.rawBody || JSON.stringify(payload)

		const rawEvent = await persistRawWebhookEvent({
			supabase,
			payload,
			headers,
			rawBody,
		})
		const jobs = classifyWebhookPayload({
			payload,
			eventContext: rawEvent.eventContext,
		})

		const { data: enqueuedCount, error: enqueueError } = await supabase.rpc(
			"enqueue_webhook_jobs",
			{
				p_event_id: rawEvent.eventId,
				p_jobs: jobs,
			},
		)
		if (enqueueError) throw enqueueError

		return res.status(200).json({
			received: true,
			eventId: rawEvent.eventId,
			enqueuedJobs: enqueuedCount || 0,
			dedupedEvent: rawEvent.deduped,
			note: "Raw webhook persisted and queued.",
		})
	} catch (error) {
		console.error("[webhook] failed to process payload", error)
		return res.status(500).json({
			error: error instanceof Error ? error.message : "Unexpected webhook processing error.",
		})
	}
})

const outboundPgmqRunner = createOutboundPgmqRunner({ supabase, config })
const messageRunner = createWebhookJobRunner({
	supabase,
	config,
	lane: "message",
	handlers: createMessageHandlers(),
	channelName: "webhook-jobs-message",
})
const appRunner = createWebhookJobRunner({
	supabase,
	config,
	lane: "app",
	handlers: createAppHandlers(),
	channelName: "webhook-jobs-app",
})
let stopOutboundRunner = null
let stopMessageRunner = null
let stopAppRunner = null
let reapTimer = null

app.listen(config.port, async () => {
	console.log(`[webhook] listening on port ${config.port}`)
	if (config.enableOutboundPgmqWorker) {
		stopOutboundRunner = await outboundPgmqRunner.start()
		console.log("[worker] outbound PGMQ worker started")
	}
	if (config.enableWebhookMessageWorker) {
		stopMessageRunner = await messageRunner.start()
		console.log("[worker] webhook message lane started")
	}
	if (config.enableWebhookAppWorker) {
		stopAppRunner = await appRunner.start()
		console.log("[worker] webhook app lane started")
	}

	reapTimer = setInterval(async () => {
		try {
			const { data, error } = await supabase.rpc("reap_stuck_webhook_jobs", {
				p_stale_after_seconds: Math.max(config.webhookLeaseSeconds * 2, 120),
			})
			if (error) throw error
			if (Number(data) > 0) {
				console.warn(`[worker] reaped ${data} stale webhook job leases`)
			}
		} catch (reapError) {
			console.error("[worker] webhook reaper failed", reapError)
		}
	}, config.webhookReapIntervalMs)
})

process.on("SIGINT", () => {
	if (typeof stopOutboundRunner === "function") stopOutboundRunner()
	if (typeof stopMessageRunner === "function") stopMessageRunner()
	if (typeof stopAppRunner === "function") stopAppRunner()
	if (reapTimer) clearInterval(reapTimer)
	process.exit(0)
})

process.on("SIGTERM", () => {
	if (typeof stopOutboundRunner === "function") stopOutboundRunner()
	if (typeof stopMessageRunner === "function") stopMessageRunner()
	if (typeof stopAppRunner === "function") stopAppRunner()
	if (reapTimer) clearInterval(reapTimer)
	process.exit(0)
})
