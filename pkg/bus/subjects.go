package bus

// Subject constants. Every harness layer is reachable through one or two of
// these. "Build your own harness" == run a worker that registers these subjects.
const (
	// Function subjects (NATS request/reply). A worker registers one of these
	// to become the implementation of that job.
	SubjHarnessTrigger = "fn.harness.trigger"      // accept a turn from a client
	SubjRunStart       = "fn.run.start"            // start the durable turn loop
	SubjAuthGetToken   = "fn.auth.get_token"       // credential vault
	SubjModelsList     = "fn.models.list"          // model catalogue
	SubjModelsGet      = "fn.models.get"           //
	SubjModelsSupports = "fn.models.supports"      //
	SubjPolicyCheck    = "fn.policy.check_permissions" // policy engine
	SubjApprovalResolve = "fn.approval.resolve"    // human-in-the-loop gate
	SubjBudgetRecord   = "fn.budget.record"        // spend tracker
	SubjBudgetCheck    = "fn.budget.check"         //
	SubjSkillsGet      = "fn.directory.skills.get"  // skill bodies
	SubjSkillsList     = "fn.directory.skills.list" //
	SubjHookPublish    = "fn.hook.publish_collect"  // before/after hook fanout

	// Provider stream subjects are dynamic: fn.provider.<name>.stream.
	// Tool subjects are dynamic: fn.tool.<name>.
)

// ProviderStreamSubject is the function subject for a provider worker.
func ProviderStreamSubject(provider string) string { return "fn.provider." + provider + ".stream" }

// ToolSubject is the function subject for a tool worker.
func ToolSubject(name string) string { return "fn.tool." + name }

// EventSubject is the JetStream subject a session's events are published to.
func EventSubject(sessionID string) string { return "evt." + sessionID }

// Stream / bucket names.
const (
	StreamEvents = "AGENT_EVENTS" // captures evt.>
	StreamTurns  = "TURN_STEPS"   // work queue, captures turn.step.>

	BucketSessions  = "sessions"
	BucketTurnState = "turn_state"
	BucketApprovals = "approvals"
	BucketBudgets   = "budgets"
	BucketTools     = "tools"
	BucketSkills    = "skills"

	ObjectBlobs = "blobs"
)
