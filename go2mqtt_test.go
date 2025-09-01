package mqtt

import (
	"errors"
	"testing"
	"time"

	paho_mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// mockClient implements paho_mqtt.Client for testing
type mockClient struct {
	connected         bool
	publishCalled     bool
	publishTopic      string
	publishPayload    []byte
	publishToken      *mockToken
	connectToken      *mockToken
	connectCalled     bool
	optionsReaderMock paho_mqtt.ClientOptionsReader
}

func (m *mockClient) IsConnected() bool      { return m.connected }
func (m *mockClient) IsConnectionOpen() bool { return m.connected }
func (m *mockClient) Publish(topic string, qos byte, retained bool, payload any) paho_mqtt.Token {
	m.publishCalled = true
	m.publishTopic = topic
	m.publishPayload = payload.([]byte)
	return m.publishToken
}
func (m *mockClient) Connect() paho_mqtt.Token {
	m.connectCalled = true
	if m.connectToken == nil {
		panic("connectToken not set in mockClient")
	}
	m.connected = m.connectToken.waitTimeoutResult && m.connectToken.err == nil
	return m.connectToken
}
func (m *mockClient) OptionsReader() paho_mqtt.ClientOptionsReader {
	return m.optionsReaderMock
}

func (m *mockClient) AddRoute(topic string, handler paho_mqtt.MessageHandler) {
	panic("unimplemented")
}
func (m *mockClient) Disconnect(quiesce uint) {
	panic("unimplemented")
}
func (m *mockClient) Subscribe(topic string, qos byte, callback paho_mqtt.MessageHandler) paho_mqtt.Token {
	panic("unimplemented")
}
func (m *mockClient) SubscribeMultiple(filters map[string]byte, callback paho_mqtt.MessageHandler) paho_mqtt.Token {
	panic("unimplemented")
}
func (m *mockClient) Unsubscribe(topics ...string) paho_mqtt.Token {
	panic("unimplemented")
}

// --- Additional tests ---

func TestEnsureConnected_ConnectCalledWhenNotConnected(t *testing.T) {
	logger := setupLogger()
	connectToken := &mockToken{waitTimeoutResult: true, err: nil}
	client := &mockClient{connected: false, connectToken: connectToken}
	bp := newBrokerProxyForTest(client, logger)
	bp.client = client
	bp.cfg.ConnectionOptions = nil // force basic options

	bp.ensureConnected()
	if !client.connectCalled {
		t.Error("ensureConnected should call Connect when not connected")
	}
}

func TestEnsureConnected_TimeoutOnConnect(t *testing.T) {
	logger := setupLogger()
	connectToken := &mockToken{waitTimeoutResult: false, err: nil}
	client := &mockClient{connected: false, connectToken: connectToken}
	bp := newBrokerProxyForTest(client, logger)
	bp.client = client

	bp.ensureConnected()
	if bp.client != nil {
		t.Error("ensureConnected should set client to nil on connect timeout")
	}
}

func TestEnsureConnected_ErrorOnConnect(t *testing.T) {
	logger := setupLogger()
	connectToken := &mockToken{waitTimeoutResult: true, err: errors.New("connect error")}
	client := &mockClient{connected: false, connectToken: connectToken}
	bp := newBrokerProxyForTest(client, logger)
	bp.client = client

	bp.ensureConnected()
	if bp.client != nil {
		t.Error("ensureConnected should set client to nil on connect error")
	}
}

func TestPublish_CallsEnsureConnected(t *testing.T) {
	logger := setupLogger()
	connectToken := &mockToken{waitTimeoutResult: true, err: nil}
	publishToken := &mockToken{waitTimeoutResult: true, err: nil}
	client := &mockClient{connected: false, connectToken: connectToken, publishToken: publishToken}
	bp := newBrokerProxyForTest(client, logger)
	bp.client = client

	bp.Publish("test/topic", []byte("payload"))
	if !client.connectCalled {
		t.Error("Publish should call ensureConnected, which calls Connect")
	}
}

func TestPublish_ConnectionTokenError(t *testing.T) {
	logger := setupLogger()
	connectToken := &mockToken{waitTimeoutResult: true, err: errors.New("connect error")}
	client := &mockClient{connected: false, connectToken: connectToken}
	bp := newBrokerProxyForTest(client, logger)
	bp.client = client

	bp.Publish("test/topic", []byte("payload"))
	if client.publishCalled {
		t.Error("Publish should not call client's Publish if connection fails")
	}
}

func TestPublish_ConnectionTokenTimeout(t *testing.T) {
	logger := setupLogger()
	connectToken := &mockToken{waitTimeoutResult: false, err: nil}
	client := &mockClient{connected: false, connectToken: connectToken}
	bp := newBrokerProxyForTest(client, logger)
	bp.client = client

	bp.Publish("test/topic", []byte("payload"))
	if client.publishCalled {
		t.Error("Publish should not call client's Publish if connection times out")
	}
}

func TestPublish_PublishTokenErrorAfterConnect(t *testing.T) {
	logger := setupLogger()
	connectToken := &mockToken{waitTimeoutResult: true, err: nil}
	publishToken := &mockToken{waitTimeoutResult: true, err: errors.New("publish error")}
	client := &mockClient{connected: true, connectToken: connectToken, publishToken: publishToken}
	bp := newBrokerProxyForTest(client, logger)
	bp.client = client

	bp.Publish("test/topic", []byte("payload"))
	if !client.publishCalled {
		t.Error("Publish should call client's Publish even if publish token has error")
	}
}

func TestPublish_PublishTokenTimeoutAfterConnect(t *testing.T) {
	logger := setupLogger()
	connectToken := &mockToken{waitTimeoutResult: true, err: nil}
	publishToken := &mockToken{waitTimeoutResult: false, err: nil}
	client := &mockClient{connected: true, connectToken: connectToken, publishToken: publishToken}
	bp := newBrokerProxyForTest(client, logger)
	bp.client = client

	bp.Publish("test/topic", []byte("payload"))
	if !client.publishCalled {
		t.Error("Publish should call client's Publish even if publish token times out")
	}
}

// mockToken implements paho_mqtt.Token for testing
type mockToken struct {
	waitTimeoutResult bool
	err               error
}

func (t *mockToken) Wait() bool                       { return true }
func (t *mockToken) WaitTimeout(d time.Duration) bool { return t.waitTimeoutResult }
func (t *mockToken) Error() error                     { return t.err }
func (t *mockToken) Done() <-chan struct{}            { return nil }

func setupLogger() *zerolog.Logger {
	l := log.Logger
	return &l
}

func newBrokerProxyForTest(client paho_mqtt.Client, logger *zerolog.Logger) *BrokerProxy {
	return &BrokerProxy{
		cfg: Config{
			BrokerURI: "tcp://localhost:1883",
			ClientID:  "test-client",
			Timeout:   1 * time.Second,
			Logger:    logger,
		},
		client: client,
	}
}

func TestPublish_SuccessfulPublish(t *testing.T) {
	logger := setupLogger()
	token := &mockToken{waitTimeoutResult: true, err: nil}
	client := &mockClient{connected: true, publishToken: token}
	bp := newBrokerProxyForTest(client, logger)

	bp.Publish("test/topic", []byte("hello"))
	if !client.publishCalled {
		t.Error("Publish should call client's Publish")
	}
	if client.publishTopic != "test/topic" {
		t.Errorf("Expected topic 'test/topic', got '%s'", client.publishTopic)
	}
	if string(client.publishPayload) != "hello" {
		t.Errorf("Expected payload 'hello', got '%s'", string(client.publishPayload))
	}
}

func TestPublish_NotConnected(t *testing.T) {
	logger := setupLogger()
	connectToken := &mockToken{waitTimeoutResult: true, err: nil}
	publishToken := &mockToken{waitTimeoutResult: true, err: nil}
	client := &mockClient{connected: false, connectToken: connectToken, publishToken: publishToken}
	bp := newBrokerProxyForTest(client, logger)

	bp.Publish("test/topic", []byte("data"))
	if !client.connectCalled {
		t.Error("Publish should call Connect when not connected")
	}
	if !client.publishCalled {
		t.Error("Publish should call client's Publish method when not connected after connecting")
	}
}

func TestPublish_TokenError(t *testing.T) {
	logger := setupLogger()
	token := &mockToken{waitTimeoutResult: true, err: errors.New("publish error")}
	client := &mockClient{connected: true, publishToken: token}
	bp := newBrokerProxyForTest(client, logger)

	bp.Publish("test/topic", []byte("fail"))
	if !client.publishCalled {
		t.Error("Publish should call client's Publish method in case of an error")
	}
}

func TestPublish_Timeout(t *testing.T) {
	logger := setupLogger()
	token := &mockToken{waitTimeoutResult: false, err: nil}
	client := &mockClient{connected: true, publishToken: token}
	bp := newBrokerProxyForTest(client, logger)

	bp.Publish("test/topic", []byte("timeout"))
	if !client.publishCalled {
		t.Error("Publish should call client's Publish")
	}
}

func TestPublish_EmptyPayload(t *testing.T) {
	logger := setupLogger()
	token := &mockToken{waitTimeoutResult: true, err: nil}
	client := &mockClient{connected: true, publishToken: token}
	bp := newBrokerProxyForTest(client, logger)

	bp.Publish("test/topic", []byte{})
	if !client.publishCalled {
		t.Error("Publish should call client's Publish for empty payload")
	}
	if len(client.publishPayload) != 0 {
		t.Errorf("Expected empty payload, got '%s'", string(client.publishPayload))
	}
}

func TestPublish_MultipleMessages(t *testing.T) {
	logger := setupLogger()
	token := &mockToken{waitTimeoutResult: true, err: nil}
	client := &mockClient{connected: true, publishToken: token}
	bp := newBrokerProxyForTest(client, logger)

	for range 3 {
		bp.Publish("test/topic", []byte("msg"))
	}
	if bp.messageCounter != 3 {
		t.Errorf("Expected messageCounter to be 3, got %d", bp.messageCounter)
	}
}

func TestPublish_NilClient(t *testing.T) {
	logger := setupLogger()
	bp := newBrokerProxyForTest(nil, logger)

	bp.Publish("test/topic", []byte("nil"))
	// Should not panic or call Publish
}
