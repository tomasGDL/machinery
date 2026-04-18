package retry

import (
	"fmt"
	"time"

	"github.com/RichardKnop/machinery/v2/log"
)

// Closure - a useful closure we can use when there is a problem
// connecting to the broker. It uses Fibonacci sequence to space out retry attempts
var Closure = func() func(chan int) {
	retryIn := 0
	fibonacci := Fibonacci()
	return func(stopChan chan int) {
		if retryIn > 0 {
			durationString := fmt.Sprintf("%vs", retryIn)
			duration, err := time.ParseDuration(durationString)
			if err != nil {
				// 解析失败时使用默认值 1 秒
				duration = time.Second
				log.GetLogger().Errorf("Parse duration error: %s, using default 1s", err.Error())
			}

			log.GetLogger().Warnf("Retrying in %v seconds", retryIn)

			select {
			case <-stopChan:
				break
			case <-time.After(duration):
				break
			}
		}
		retryIn = fibonacci()
	}
}
