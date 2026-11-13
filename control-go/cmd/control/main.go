package main

import (
	"fmt"

	"github.com/aegisrun/aegisrun/control-go/pkg/pb"
)

func main() {
	status := pb.WorkerStatus_HEALTHY
	fmt.Printf("control-go initialized (worker_status=%s)\n", status.String())
}

