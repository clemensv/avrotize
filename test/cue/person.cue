package demo

#Address: {
  street: string
  zip?: int
}

#Person: {
  name: string
  age?: int
  rating: float
  score: number
  active: bool
  data: bytes
  nothing: null
  address: #Address
  tags: [...string]
  labels: { [string]: string }
  status: "new" | "active" | "closed"
  choice: string | int | null
  role: string | *"user"
  nested: {
    value: int
  }
}
