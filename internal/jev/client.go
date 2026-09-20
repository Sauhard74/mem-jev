package jev

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	FailureAuthentication  = "authentication"
	FailureThrottled       = "throttled"
	FailureOverloaded      = "overloaded"
	FailureProvider        = "provider_error"
	FailureOversized       = "oversized_response"
	FailureInvalidResponse = "invalid_response"
	FailureTimeout         = "timeout"
	FailureCancelled       = "cancelled"
	FailureTransport       = "transport"
)

type ProviderError struct {
	Code       string
	StatusCode int
	Err        error
}

func (err *ProviderError) Error() string {
	if err == nil {
		return "Jev provider error"
	}
	if err.StatusCode != 0 {
		return fmt.Sprintf("Jev provider %s (HTTP %d)", err.Code, err.StatusCode)
	}
	return "Jev provider " + err.Code
}

func (err *ProviderError) Unwrap() error { return err.Err }

type ClientConfig struct {
	Endpoint, AllowedHost string
	Model                 string
	Rubric                RubricManifest
	Secret                SecretSource
	HTTPClient            *http.Client
	Timeout               time.Duration
	MaximumRequestBytes   int64
	MaximumResponseBytes  int64
}

type Client struct {
	endpoint             string
	model                string
	rubric               RubricManifest
	secret               SecretSource
	http                 *http.Client
	timeout              time.Duration
	maximumRequestBytes  int64
	maximumResponseBytes int64
}

type Usage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

type Evaluation struct {
	Model    string
	Judgment Judgment
	Usage    Usage
}

func NewClient(config ClientConfig) (*Client, error) {
	endpoint, err := url.Parse(strings.TrimSpace(config.Endpoint))
	allowedHost := strings.TrimSpace(config.AllowedHost)
	if err != nil || endpoint.Scheme != "https" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Host == "" || endpoint.Host != allowedHost || endpoint.Path == "" || config.Secret == nil || config.HTTPClient == nil || config.Timeout <= 0 || config.MaximumRequestBytes < 1_024 || config.MaximumRequestBytes > 16<<20 || config.MaximumResponseBytes < 32 || config.MaximumResponseBytes > 16<<20 || strings.TrimSpace(config.Model) != config.Rubric.Model || ValidateRubricManifest(config.Rubric) != nil {
		return nil, fmt.Errorf("invalid Jev client configuration")
	}
	httpClient := *config.HTTPClient
	httpClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	httpClient.Timeout = 0 // context owns the one authoritative deadline
	return &Client{endpoint: endpoint.String(), model: config.Model, rubric: config.Rubric, secret: config.Secret, http: &httpClient, timeout: config.Timeout, maximumRequestBytes: config.MaximumRequestBytes, maximumResponseBytes: config.MaximumResponseBytes}, nil
}

type providerQuestion struct {
	Type         QuestionType `json:"type"`
	Instructions string       `json:"instructions"`
	Criteria     any          `json:"criteria"`
}

type providerRequest struct {
	State     any                         `json:"state"`
	Model     string                      `json:"model"`
	Questions map[string]providerQuestion `json:"questions"`
}

func (client *Client) Evaluate(ctx context.Context, state any) (Evaluation, error) {
	if client == nil {
		return Evaluation{}, &ProviderError{Code: FailureTransport}
	}
	token, err := client.secret.Token()
	if err != nil {
		return Evaluation{}, &ProviderError{Code: FailureAuthentication, Err: ErrSecretUnavailable}
	}
	questions := make(map[string]providerQuestion, len(client.rubric.Questions))
	for _, question := range client.rubric.Questions {
		var criteria any
		if question.Type == QuestionNoul {
			criteria = map[string]string{"false": question.Criteria[0], "true": question.Criteria[1]}
		} else {
			criteria = append([]string(nil), question.Criteria...)
		}
		questions[question.ID] = providerQuestion{Type: question.Type, Instructions: question.Instructions, Criteria: criteria}
	}
	body, err := json.Marshal(providerRequest{State: state, Model: client.model, Questions: questions})
	if err != nil || int64(len(body)) > client.maximumRequestBytes {
		return Evaluation{}, &ProviderError{Code: FailureOversized}
	}
	requestContext, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, client.endpoint, bytes.NewReader(body))
	if err != nil {
		return Evaluation{}, &ProviderError{Code: FailureTransport}
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := client.http.Do(request)
	if err != nil {
		if errors.Is(requestContext.Err(), context.DeadlineExceeded) {
			return Evaluation{}, &ProviderError{Code: FailureTimeout, Err: context.DeadlineExceeded}
		}
		if errors.Is(requestContext.Err(), context.Canceled) {
			return Evaluation{}, &ProviderError{Code: FailureCancelled, Err: context.Canceled}
		}
		return Evaluation{}, &ProviderError{Code: FailureTransport}
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
		return Evaluation{}, statusError(response.StatusCode)
	}
	limited := io.LimitReader(response.Body, client.maximumResponseBytes+1)
	responseBody, err := io.ReadAll(limited)
	if err != nil {
		return Evaluation{}, &ProviderError{Code: FailureTransport}
	}
	if int64(len(responseBody)) > client.maximumResponseBytes {
		return Evaluation{}, &ProviderError{Code: FailureOversized}
	}
	result, err := decodeEvaluation(responseBody, client.rubric)
	if err != nil {
		return Evaluation{}, &ProviderError{Code: FailureInvalidResponse}
	}
	return result, nil
}

func statusError(status int) error {
	code := FailureProvider
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		code = FailureAuthentication
	case http.StatusTooManyRequests:
		code = FailureThrottled
	case 529:
		code = FailureOverloaded
	}
	return &ProviderError{Code: code, StatusCode: status}
}

type responseEnvelope struct {
	Model   string                     `json:"model"`
	Answers map[string]json.RawMessage `json:"answers"`
	Usage   Usage                      `json:"usage"`
}

func decodeEvaluation(body []byte, rubric RubricManifest) (Evaluation, error) {
	var envelope responseEnvelope
	if err := decodeStrict(body, &envelope); err != nil || envelope.Model != rubric.Model || len(envelope.Answers) != len(rubric.Questions) || envelope.Usage.InputTokens < 0 || envelope.Usage.OutputTokens < 0 || envelope.Usage.InputTokens > 100_000_000 || envelope.Usage.OutputTokens > 100_000_000 {
		return Evaluation{}, ErrInvalidJudgment
	}
	answers := make(map[string]any, len(rubric.Questions))
	for _, question := range rubric.Questions {
		raw, ok := envelope.Answers[question.ID]
		if !ok {
			return Evaluation{}, ErrInvalidJudgment
		}
		if question.Type == QuestionNoul {
			answer, err := decodeNoul(raw)
			if err != nil {
				return Evaluation{}, err
			}
			answers[question.ID] = answer
			continue
		}
		answer, err := decodeScore(raw, question.Criteria)
		if err != nil {
			return Evaluation{}, err
		}
		answers[question.ID] = answer
	}
	judgment := Judgment{
		IntentFit: answers["intent_fit"].(ScoreAnswer), PreconditionsLikelySatisfied: answers["preconditions_likely_satisfied"].(NoulAnswer),
		TaskCoverage: answers["task_coverage"].(ScoreAnswer), ContradictsRequest: answers["contradicts_request"].(NoulAnswer), UsefulAsPartialPlan: answers["useful_as_partial_plan"].(NoulAnswer),
	}
	if _, err := judgment.Features(); err != nil {
		return Evaluation{}, err
	}
	return Evaluation{Model: envelope.Model, Judgment: judgment, Usage: envelope.Usage}, nil
}

type rawNoul struct {
	Type QuestionType `json:"type"`
	Noul json.Number  `json:"noul"`
}

func decodeNoul(raw []byte) (NoulAnswer, error) {
	var answer rawNoul
	if err := decodeStrict(raw, &answer); err != nil || answer.Type != QuestionNoul {
		return NoulAnswer{}, ErrInvalidJudgment
	}
	value, err := ProbabilityMicros(answer.Noul.String())
	if err != nil {
		return NoulAnswer{}, ErrInvalidJudgment
	}
	return NoulAnswer{NoulMicros: value}, nil
}

type rawScore struct {
	Type          QuestionType           `json:"type"`
	Score         json.Number            `json:"score"`
	Legend        map[string]string      `json:"legend"`
	Probabilities map[string]json.Number `json:"probabilities"`
	Confidence    json.Number            `json:"confidence"`
}

func decodeScore(raw []byte, criteria []string) (ScoreAnswer, error) {
	var answer rawScore
	if err := decodeStrict(raw, &answer); err != nil || answer.Type != QuestionScore || len(answer.Legend) != len(criteria) || len(answer.Probabilities) != len(criteria) {
		return ScoreAnswer{}, ErrInvalidJudgment
	}
	probabilities := make([]int32, len(criteria))
	for index, criterion := range criteria {
		key := strconv.Itoa(index)
		if answer.Legend[key] != criterion {
			return ScoreAnswer{}, ErrInvalidJudgment
		}
		rawProbability, ok := answer.Probabilities[key]
		if !ok {
			return ScoreAnswer{}, ErrInvalidJudgment
		}
		value, err := ProbabilityMicros(rawProbability.String())
		if err != nil {
			return ScoreAnswer{}, ErrInvalidJudgment
		}
		probabilities[index] = value
	}
	score, err := decimalMicros(answer.Score.String(), int32(len(criteria)-1))
	if err != nil {
		return ScoreAnswer{}, ErrInvalidJudgment
	}
	confidence, err := ProbabilityMicros(answer.Confidence.String())
	if err != nil {
		return ScoreAnswer{}, ErrInvalidJudgment
	}
	result := ScoreAnswer{ScoreMicros: score, ConfidenceMicros: confidence, ProbabilitiesMicros: probabilities}
	if !validScore(result, len(criteria)) {
		return ScoreAnswer{}, ErrInvalidJudgment
	}
	return result, nil
}

func decodeStrict(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}

// sortedAnswerIDs is retained as a small testable helper for future response
// formats whose map iteration must never affect persistence.
func sortedAnswerIDs(answers map[string]json.RawMessage) []string {
	ids := make([]string, 0, len(answers))
	for id := range answers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
