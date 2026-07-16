package helps

import (
	"strconv"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// NormalizeCodexGPT55ReplayNamespaces removes replay-only namespaces that the
// gpt-5.5 Codex upstream rejects while preserving namespaces everywhere else.
func NormalizeCodexGPT55ReplayNamespaces(body []byte, baseModel string) []byte {
	if baseModel != "gpt-5.5" || len(body) == 0 {
		return body
	}

	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return body
	}

	out := body
	for index, item := range input.Array() {
		switch item.Get("type").String() {
		case "function_call", "custom_tool_call":
			if !item.Get("namespace").Exists() {
				continue
			}
			updated, errDelete := sjson.DeleteBytes(out, "input."+strconv.Itoa(index)+".namespace")
			if errDelete == nil {
				out = updated
			}
		}
	}
	return out
}
