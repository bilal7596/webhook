function createWebhookJobRunner(args) {
	const {
		supabase,
		config,
		lane,
		handlers,
		channelName,
	} = args

	let pollTimer = null
	let isRunning = false
	let isProcessing = false

	async function finalizeSuccess(jobId) {
		const { error } = await supabase.rpc("complete_webhook_job", { p_job_id: jobId })
		if (error) throw error
	}

	async function finalizeFailure(jobId, errorMessage) {
		const { error } = await supabase.rpc("fail_webhook_job", {
			p_job_id: jobId,
			p_error: errorMessage,
			p_retry_delay_seconds: config.webhookRetryDelaySeconds,
		})
		if (error) throw error
	}

	async function processQueue(reason = "poll") {
		if (!isRunning || isProcessing) return
		isProcessing = true
		try {
			const { data, error } = await supabase.rpc("claim_webhook_jobs", {
				p_lane: lane,
				p_limit: config.claimBatchSize,
				p_worker_id: `${channelName}-${process.pid}`,
				p_lease_seconds: config.webhookLeaseSeconds,
			})
			if (error) throw error
			const rows = Array.isArray(data) ? data : []
			if (rows.length === 0) return

			for (const job of rows) {
				try {
					const handler = handlers[job.job_type] || handlers.default
					if (!handler) {
						throw new Error(`No handler registered for job type "${job.job_type}"`)
					}
					await handler({ supabase, config, job })
					await finalizeSuccess(job.id)
					console.info(`[${channelName}] completed ${job.id} (${reason})`)
				} catch (jobError) {
					const message = jobError instanceof Error ? jobError.message : String(jobError)
					await finalizeFailure(job.id, message)
					console.error(`[${channelName}] failed ${job.id} (${reason})`, message)
				}
			}
		} catch (error) {
			console.error(`[${channelName}] processQueue error`, error)
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
			.channel(channelName)
			.on(
				"postgres_changes",
				{ event: "*", schema: "public", table: "webhook_jobs" },
				(payload) => {
					const row = payload?.new || payload?.old
					if (!row) return
					if (row.lane === lane && row.status === "queued") {
						void processQueue("realtime")
					}
				},
			)
			.subscribe((status) => {
				if (status === "CHANNEL_ERROR" || status === "TIMED_OUT" || status === "CLOSED") {
					console.warn(`[${channelName}] realtime status=${status}; polling fallback active`)
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

module.exports = { createWebhookJobRunner }
