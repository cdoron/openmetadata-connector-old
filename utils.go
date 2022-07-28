package main

import "strings"

func appendStrings(a string, b string) string {
	if strings.Contains(b, ".") {
		return a + ".\"" + b + "\""
	} else {
		return a + "." + b
	}
}

func stripTag(tag string) string {
	return strings.TrimPrefix(tag, "Fybrik.")
}

func updateCustomProperty(customProperties map[string]interface{}, orig map[string]interface{}, key string, value *string) {
	if value != nil {
		customProperties[key] = *value
		return
	}
	if v, ok := orig[key]; ok {
		customProperties[key] = v
	}
}
