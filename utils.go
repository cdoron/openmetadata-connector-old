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
