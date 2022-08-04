package utils

import "strings"

func AppendStrings(a string, b string) string {
	if strings.Contains(b, ".") {
		return a + ".\"" + b + "\""
	} else {
		return a + "." + b
	}
}

func StripTag(tag string) string {
	return strings.TrimPrefix(tag, "Fybrik.")
}

func UpdateCustomProperty(customProperties map[string]interface{}, orig map[string]interface{}, key string, value *string) {
	if value != nil && *value != "" {
		customProperties[key] = *value
		return
	}
	if v, ok := orig[key]; ok && v != "" {
		customProperties[key] = v
	}
}
