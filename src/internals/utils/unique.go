package utils

func Unique[T comparable](slice []T) []T {
	keys := make(map[T]bool)
	var list []T
	for _, entry := range slice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}
