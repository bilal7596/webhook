const { createClient } = require("@supabase/supabase-js")

function createServiceClient(config) {
	if (!config.supabaseUrl || !config.supabaseServiceRoleKey) {
		throw new Error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required.")
	}

	return createClient(config.supabaseUrl, config.supabaseServiceRoleKey, {
		auth: {
			autoRefreshToken: false,
			persistSession: false,
		},
	})
}

module.exports = { createServiceClient }
