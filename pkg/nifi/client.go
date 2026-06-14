package nifi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultTimeout = 10 * time.Second

type ReachabilityChecker interface {
	CheckReachable(ctx context.Context, baseURI string, timeout time.Duration) error
}

type HTTPReachabilityChecker struct {
	Client *http.Client
}

func (c HTTPReachabilityChecker) CheckReachable(ctx context.Context, baseURI string, timeout time.Duration) error {
	if timeout == 0 {
		timeout = defaultTimeout
	}

	endpoint, err := flowAboutURL(baseURI)
	if err != nil {
		return err
	}

	client := c.Client
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 399 {
		return fmt.Errorf("nifi api returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func flowAboutURL(baseURI string) (string, error) {
	parsed, err := url.Parse(strings.TrimRight(baseURI, "/"))
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("nifi api uri must include scheme and host")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/nifi-api/flow/about"
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}
