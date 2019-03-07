package core

func sum(input []int) (result int) {
	for i := range input {
		result += input[i]
	}
	return
}