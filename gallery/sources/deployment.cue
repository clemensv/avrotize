package deploy

#Resources: {
	cpu:    string
	memory: string
}

#Port: {
	name:      string
	container: int
	protocol:  "TCP" | "UDP" | *"TCP"
}

#Service: {
	name:      string
	image:     string
	replicas:  int
	resources: #Resources
	ports: [...#Port]
	env: {[string]: string}
	tier:    "frontend" | "backend" | "data"
	public:  bool
	notes?:  string
}
