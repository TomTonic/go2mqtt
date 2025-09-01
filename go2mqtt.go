package mqtt

import (
	"sync"
	"time"

	paho_mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
)

type Config struct {
	// The URI of the MQTT broker, e.g. "tcp://localhost:1883". The format should be scheme://host:port
	// where "scheme" is one of "tcp", "ssl", or "ws", "host" is the ip-address (or hostname) and "port"
	// is the port on which the broker is accepting connections.
	BrokerURI string
	// The client ID to use when connecting to the broker. According to the MQTT v3.1 specification, a
	// client id must be no longer than 23 characters.
	ClientID string
	// Use this to set additional connection options like username, password, TLS config, etc.
	// See https://pkg.go.dev/github.com/eclipse/paho.mqtt.golang#ClientOptions for details.
	// If this is set, the options BrokerURI and ClientID are ignored.
	ConnectionOptions *ConnectionOptions
	// Timeout for connecting and publishing messages. If not set, defaults to 10 seconds.
	// After this time, the operation is regarded as failed and a warning is logged.
	Timeout time.Duration
	// Logger to use for logging connection and publishing status.
	Logger *zerolog.Logger
}

type BrokerProxy struct {
	cfg            Config
	client         paho_mqtt.Client
	messageCounter int
	mu             sync.Mutex
}

type ConnectionOptions = paho_mqtt.ClientOptions

func New(cfg Config) *BrokerProxy {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	result := &BrokerProxy{cfg: cfg}
	result.ensureConnected()
	return result
}

func (m *BrokerProxy) ensureConnected() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client != nil && m.client.IsConnected() {
		return
	}

	if m.client == nil {
		opts := m.cfg.ConnectionOptions
		if opts == nil {
			m.cfg.Logger.Info().
				Str("BrokerURI", m.cfg.BrokerURI).
				Str("ClientID", m.cfg.ClientID).
				Msg("Connecting to MQTT broker with basic options")
			opts = paho_mqtt.NewClientOptions().
				AddBroker(m.cfg.BrokerURI).
				SetClientID(m.cfg.ClientID).
				SetAutoReconnect(true)
		} else {
			m.cfg.Logger.Info().
				Str("Servers", func() string {
					brokers := opts.Servers
					if len(brokers) == 0 {
						return "<none>"
					}
					result := ""
					for i, b := range brokers {
						if i > 0 {
							result += ";"
						}
						result += b.String()
					}
					return result
				}()).
				Str("ClientID", opts.ClientID).
				Str("Username", opts.Username).
				Str("Password", func() string {
					if opts.Password != "" {
						return "<set>"
					}
					return "<empty>"
				}()).
				Msg("Connecting to MQTT broker with custom options")
		}
		m.client = paho_mqtt.NewClient(opts)
	}

	token := m.client.Connect()

	if !token.WaitTimeout(m.cfg.Timeout) {
		m.cfg.Logger.Warn().
			Str("Timeout", m.cfg.Timeout.String()).
			Msg("Timeout while connecting to MQTT broker")
		m.client = nil
		return
	}
	if token.Error() != nil {
		m.cfg.Logger.Warn().
			Err(token.Error()).
			Msg("Error connecting to MQTT broker")
		m.client = nil
		return
	}
	m.cfg.Logger.Info().
		Msg("Successfully connected to MQTT broker")
	if m.cfg.Logger.GetLevel() == zerolog.DebugLevel {
		or := m.client.OptionsReader()
		m.cfg.Logger.Debug().
			Str("Servers", func() string {
				brokers := or.Servers()
				if len(brokers) == 0 {
					return "<none>"
				}
				result := ""
				for i, b := range brokers {
					if i > 0 {
						result += ";"
					}
					result += b.String()
				}
				return result
			}()).
			Str("ClientID", or.ClientID()).
			Str("Username", or.Username()).
			Str("Password", func() string {
				if or.Password() != "" {
					return "<set>"
				}
				return "<empty>"
			}()).
			Bool("AutoReconnect", or.AutoReconnect()).
			Bool("CleanSession", or.CleanSession()).
			Bool("ConnectRetry", or.ConnectRetry()).
			Str("ConnectRetryInterval", or.ConnectRetryInterval().String()).
			Str("ConnectTimeout", or.ConnectTimeout().String()).
			Str("HTTPHeaders", func() string {
				headers := or.HTTPHeaders()
				if len(headers) == 0 {
					return "<none>"
				}
				result := ""
				for k, v := range headers {
					result += k + ": " + v[0]
					for i := 1; i < len(v); i++ {
						result += "," + v[i]
					}
					result += ";"
				}
				if len(result) > 0 && result[len(result)-1] == ';' {
					result = result[:len(result)-1]
				}
				return result
			}()).
			Str("KeepAlive", or.KeepAlive().String()).
			Str("MaxReconnectInterval", or.MaxReconnectInterval().String()).
			Int("MessageChannelDepth", int(or.MessageChannelDepth())).
			Bool("Order", or.Order()).
			Str("PingTimeout", or.PingTimeout().String()).
			Int("ProtocolVersion", int(or.ProtocolVersion())).
			Bool("ResumeSubs", or.ResumeSubs()).
			Str("TLSConfig", func() string {
				if or.TLSConfig() != nil {
					return "<set>"
				}
				return "<empty>"
			}()).
			Str("WebsocketOptions", func() string {
				if or.WebsocketOptions() != nil {
					return "<set>"
				}
				return "<empty>"
			}()).
			Bool("WillEnabled", or.WillEnabled()).
			Str("WillPayload", func() string {
				if len(or.WillPayload()) > 0 {
					return string(or.WillPayload())
				}
				return "<empty>"
			}()).
			Int("WillQos", int(or.WillQos())).
			Bool("WillRetained", or.WillRetained()).
			Str("WillTopic", or.WillTopic()).
			Str("WriteTimeout", or.WriteTimeout().String()).
			Msg("Effective MQTT connection settings")
	}
}

func (m *BrokerProxy) Publish(topic string, payload []byte) {
	m.messageCounter++

	m.cfg.Logger.Debug().
		Str("Topic", topic).
		Int("MsgNumber", m.messageCounter).
		Str("payload", string(payload)).
		Msg("Publishing message to MQTT broker...")

	m.ensureConnected()

	if m.client == nil || !m.client.IsConnected() {
		m.cfg.Logger.Warn().
			Str("Topic", topic).
			Int("MsgNumber", m.messageCounter).
			Msg("Not connected to MQTT broker - Cannot publish message to topic - Message discarded")
		return
	}

	token := m.client.Publish(topic, 0, false, payload)
	successfully_sent := token.WaitTimeout(m.cfg.Timeout)

	err := token.Error()
	if err != nil {
		m.cfg.Logger.Warn().
			Str("Topic", topic).
			Int("MsgNumber", m.messageCounter).
			Err(err).
			Msg("Error publishing MQTT message to topic - Message discarded")
		return
	}

	if successfully_sent {
		m.cfg.Logger.Info().
			Str("Topic", topic).
			Int("MsgNumber", m.messageCounter).
			Msg("Successfully published MQTT message to topic")
		return
	}

}
