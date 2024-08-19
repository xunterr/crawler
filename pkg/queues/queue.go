package queues

type Queue[T any] []T

func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{}
}

func (q *Queue[T]) Push(el T) {
	*q = append(*q, el)
}

func (q *Queue[T]) Peek() (T, bool) {
	if len(*q) == 0 {
		return *new(T), false
	}

	return (*q)[0], true
}

func (q *Queue[T]) Pop() (T, bool) {
	if len(*q) == 0 {
		return *new(T), false
	}

	el := (*q)[0]

	*q = (*q)[1:len(*q)]

	return el, true
}
