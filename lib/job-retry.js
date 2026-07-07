function computeRetryDelaySeconds(attemptCount, baseDelaySeconds = 30) {
	const normalizedAttempt = Math.max(Number(attemptCount) || 1, 1)
	const baseDelay = Math.max(Number(baseDelaySeconds) || 30, 1)
	const exponential = baseDelay * 2 ** Math.max(normalizedAttempt - 1, 0)
	return Math.min(Math.max(exponential, 1), 3600)
}

function isTerminalFailure(attempt, maxAttempts) {
	const normalizedAttempt = Math.max(Number(attempt) || 1, 1)
	const normalizedMax = Math.max(Number(maxAttempts) || 5, 1)
	return normalizedAttempt >= normalizedMax
}

function nextAttemptPayload(payload, errorMessage) {
	const attempt = Math.max(Number(payload?.attempt) || 1, 1)
	const maxAttempts = Math.max(Number(payload?.max_attempts) || 5, 1)
	return {
		...payload,
		attempt: attempt + 1,
		max_attempts: maxAttempts,
		last_error: errorMessage,
		requeued_at: new Date().toISOString(),
	}
}

module.exports = {
	computeRetryDelaySeconds,
	isTerminalFailure,
	nextAttemptPayload,
}
