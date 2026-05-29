// Package directory is the skills directory worker. It serves per-function
// usage documents (skills) out of the BucketSkills KV bucket: a worker can
// fetch the body of an unfamiliar tool/skill before invoking it. This is the
// layer a private-artifact-store worker would replace by registering the same
// subjects.
package directory

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/epuerta/vent/pkg/bus"
	"github.com/epuerta/vent/pkg/types"
)

// skillKey builds a KV-safe key from a skill's namespace and name. NATS KV
// keys only permit [-/_=.a-zA-Z0-9], so the namespace/name are joined with a
// '.' separator (the ':' used in display form is not a legal KV key char).
func skillKey(namespace, name string) string {
	return namespace + "." + name
}

// seeds are written to the bucket on Start when it is empty.
var seeds = []types.Skill{
	{
		Namespace: "vent",
		Name:      "identity",
		Body:      "You are a vent agent. Tools are functions on a bus; before calling an unfamiliar tool, fetch its skill via directory::skills::get.",
	},
	{
		Namespace: "vent",
		Name:      "bash",
		Body:      "The bash tool runs a shell command and returns combined stdout/stderr. Prefer non-interactive commands.",
	},
}

// Start registers the directory worker's subjects on the bus and seeds a couple
// of starter skills when the BucketSkills bucket is empty. It is non-blocking:
// the registrations live for the lifetime of the connection.
func Start(ctx context.Context, b *bus.Bus) error {
	if err := seedIfEmpty(ctx, b); err != nil {
		return fmt.Errorf("seed skills: %w", err)
	}

	if _, err := b.Register(bus.SubjSkillsGet, func(ctx context.Context, data []byte) (any, error) {
		var req types.SkillsGetRequest
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, fmt.Errorf("decode request: %w", err)
		}
		var sk types.Skill
		found, err := b.GetJSON(ctx, bus.BucketSkills, skillKey(req.Namespace, req.Name), &sk)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("skill not found: %s::%s", req.Namespace, req.Name)
		}
		return sk, nil
	}); err != nil {
		return fmt.Errorf("register %s: %w", bus.SubjSkillsGet, err)
	}

	if _, err := b.Register(bus.SubjSkillsList, func(ctx context.Context, _ []byte) (any, error) {
		return listSkills(ctx, b)
	}); err != nil {
		return fmt.Errorf("register %s: %w", bus.SubjSkillsList, err)
	}

	return nil
}

// seedIfEmpty writes the starter skills only when none of them already exist.
func seedIfEmpty(ctx context.Context, b *bus.Bus) error {
	var probe types.Skill
	first := seeds[0]
	found, err := b.GetJSON(ctx, bus.BucketSkills, skillKey(first.Namespace, first.Name), &probe)
	if err != nil {
		return err
	}
	if found {
		return nil
	}
	for _, sk := range seeds {
		if err := b.PutJSON(ctx, bus.BucketSkills, skillKey(sk.Namespace, sk.Name), sk); err != nil {
			return err
		}
	}
	return nil
}

// listSkills reads every key in the skills bucket and returns the decoded skills.
func listSkills(ctx context.Context, b *bus.Bus) ([]types.Skill, error) {
	kv, err := b.KV(ctx, bus.BucketSkills)
	if err != nil {
		return nil, err
	}
	keys, err := kv.Keys(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]types.Skill, 0, len(keys))
	for _, key := range keys {
		var sk types.Skill
		found, err := b.GetJSON(ctx, bus.BucketSkills, key, &sk)
		if err != nil {
			return nil, err
		}
		if found {
			out = append(out, sk)
		}
	}
	return out, nil
}
