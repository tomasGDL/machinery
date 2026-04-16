package machinery

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/RichardKnop/machinery/v2/config"

	brokeriface "github.com/RichardKnop/machinery/v2/brokers/iface"
	redisbroker "github.com/RichardKnop/machinery/v2/brokers/redis"

	backendiface "github.com/RichardKnop/machinery/v2/backends/iface"
	redisbackend "github.com/RichardKnop/machinery/v2/backends/redis"

	lockiface "github.com/RichardKnop/machinery/v2/locks/iface"
	redislock "github.com/RichardKnop/machinery/v2/locks/redis"
)

// BrokerFactory creates a new Broker object that only supports Redis
func BrokerFactory(cnf *config.Config) (brokeriface.Broker, error) {
	if strings.HasPrefix(cnf.Broker, "redis://") || strings.HasPrefix(cnf.Broker, "rediss://") {
		var scheme string
		if strings.HasPrefix(cnf.Broker, "redis://") {
			scheme = "redis://"
		} else {
			scheme = "rediss://"
		}
		parts := strings.Split(cnf.Broker, scheme)
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"Redis broker connection string should be in format %shost:port, instead got %s", scheme,
				cnf.Broker,
			)
		}
		brokers := strings.Split(parts[1], ",")
		if len(brokers) > 1 || (cnf.Redis != nil && cnf.Redis.ClusterMode) {
			return redisbroker.NewGR(cnf, brokers, 0), nil
		}
		redisHost, redisPassword, redisDB, err := ParseRedisURL(cnf.Broker)
		if err != nil {
			return nil, err
		}
		return redisbroker.New(cnf, redisHost, redisPassword, "", redisDB), nil
	}

	if strings.HasPrefix(cnf.Broker, "redis+socket://") {
		redisSocket, redisPassword, redisDB, err := ParseRedisSocketURL(cnf.Broker)
		if err != nil {
			return nil, err
		}

		return redisbroker.New(cnf, "", redisPassword, redisSocket, redisDB), nil
	}

	return nil, fmt.Errorf("Factory failed with broker URL: %v (only redis:// and redis+socket:// are supported)", cnf.Broker)
}

// BackendFactory creates a new Backend object that only supports Redis
func BackendFactory(cnf *config.Config) (backendiface.Backend, error) {
	if strings.HasPrefix(cnf.ResultBackend, "redis://") || strings.HasPrefix(cnf.ResultBackend, "rediss://") {
		var scheme string
		if strings.HasPrefix(cnf.ResultBackend, "redis://") {
			scheme = "redis://"
		} else {
			scheme = "rediss://"
		}
		parts := strings.Split(cnf.ResultBackend, scheme)
		addrs := strings.Split(parts[1], ",")
		if len(addrs) > 1 || (cnf.Redis != nil && cnf.Redis.ClusterMode) {
			return redisbackend.NewGR(cnf, addrs, 0), nil
		}
		redisHost, redisPassword, redisDB, err := ParseRedisURL(cnf.ResultBackend)

		if err != nil {
			return nil, err
		}

		return redisbackend.New(cnf, redisHost, redisPassword, "", redisDB), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "redis+socket://") {
		redisSocket, redisPassword, redisDB, err := ParseRedisSocketURL(cnf.ResultBackend)
		if err != nil {
			return nil, err
		}

		return redisbackend.New(cnf, "", redisPassword, redisSocket, redisDB), nil
	}

	return nil, fmt.Errorf("Factory failed with result backend: %v (only redis:// and redis+socket:// are supported)", cnf.ResultBackend)
}

// LockFactory creates a new Lock object that only supports Redis
func LockFactory(cnf *config.Config) (lockiface.Lock, error) {
	if strings.HasPrefix(cnf.Lock, "redis://") {
		parts := strings.Split(cnf.Lock, "redis://")
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"Redis lock connection string should be in format redis://host:port, instead got %s",
				cnf.Lock,
			)
		}
		locks := strings.Split(parts[1], ",")
		return redislock.New(cnf, locks, 0, 3), nil
	}

	return nil, fmt.Errorf("Factory failed with lock URL: %v (only redis:// is supported)", cnf.Lock)
}

// ParseRedisURL extracts Redis connection options from a URL
func ParseRedisURL(urlStr string) (host, password string, db int, err error) {
	// redis://pwd@host/db

	var u *url.URL
	u, err = url.Parse(urlStr)
	if err != nil {
		return
	}
	if u.Scheme != "redis" && u.Scheme != "rediss" {
		err = errors.New("No redis scheme found")
		return
	}

	if u.User != nil {
		var exists bool
		password, exists = u.User.Password()
		if !exists {
			password = u.User.Username()
		}
	}

	host = u.Host

	parts := strings.Split(u.Path, "/")
	if len(parts) == 1 {
		db = 0 //default redis db
	} else {
		db, err = strconv.Atoi(parts[1])
		if err != nil {
			db, err = 0, nil //ignore err here
		}
	}

	return
}

// ParseRedisSocketURL extracts Redis connection options from a URL with the
// redis+socket:// scheme. This scheme is not standard (or even de facto) and
// is used as a transitional mechanism until the the config package gains the
// proper facilities to support socket-based connections.
func ParseRedisSocketURL(urlStr string) (path, password string, db int, err error) {
	parts := strings.Split(urlStr, "redis+socket://")
	if parts[0] != "" {
		err = errors.New("No redis scheme found")
		return
	}

	// redis+socket://password@/path/to/file.soc:/db

	if len(parts) != 2 {
		err = fmt.Errorf("Redis socket connection string should be in format redis+socket://password@/path/to/file.sock:/db, instead got %s", urlStr)
		return
	}

	remainder := parts[1]

	// Extract password if any
	parts = strings.SplitN(remainder, "@", 2)
	if len(parts) == 2 {
		password = parts[0]
		remainder = parts[1]
	} else {
		remainder = parts[0]
	}

	// Extract path
	parts = strings.SplitN(remainder, ":", 2)
	path = parts[0]
	if path == "" {
		err = fmt.Errorf("Redis socket connection string should be in format redis+socket://password@/path/to/file.sock:/db, instead got %s", urlStr)
		return
	}
	if len(parts) == 2 {
		remainder = parts[1]
	}

	// Extract DB if any
	parts = strings.SplitN(remainder, "/", 2)
	if len(parts) == 2 {
		db, _ = strconv.Atoi(parts[1])
	}

	return
}
