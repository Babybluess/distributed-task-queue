package task

const DefaultQueue = "default"

type Router struct {
	routes map[string]string
}

func NewRouter() *Router {
	return &Router{routes: make(map[string]string)}
}

func (r *Router) Route(taskType, queue string) {
	r.routes[taskType] = queue
}

func (r *Router) QueueFor(taskType string) string {
	if q, ok := r.routes[taskType]; ok {
		return q
	}
	return DefaultQueue
}
