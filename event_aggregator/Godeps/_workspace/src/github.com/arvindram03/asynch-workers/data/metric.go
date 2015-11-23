package data

type Metric struct {
	Username string `json:"username"`
	Count    int64  `json:"count"`
	Metric   string `json:"metric"`
}
