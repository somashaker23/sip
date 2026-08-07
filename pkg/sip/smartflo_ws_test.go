package sip

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	msdk "github.com/livekit/media-sdk"
	"github.com/livekit/protocol/logger"
	"github.com/stretchr/testify/require"

	"github.com/livekit/sip/pkg/config"
)

type testPCMWriter struct {
	mu         sync.Mutex
	sampleRate int
	samples    []msdk.PCM16Sample
}

func (w *testPCMWriter) String() string { return "testPCMWriter" }
func (w *testPCMWriter) SampleRate() int {
	return w.sampleRate
}
func (w *testPCMWriter) Close() error { return nil }
func (w *testPCMWriter) WriteSample(sample msdk.PCM16Sample) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	cloned := append(msdk.PCM16Sample(nil), sample...)
	w.samples = append(w.samples, cloned)
	return nil
}
func (w *testPCMWriter) LastSample() msdk.PCM16Sample {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.samples) == 0 {
		return nil
	}
	return append(msdk.PCM16Sample(nil), w.samples[len(w.samples)-1]...)
}

func TestResolveSmartfloWSConfig(t *testing.T) {
	cfg, err := resolveSmartfloWSConfig("call-1", 8000, &config.SmartfloWSConfig{
		Enabled:     true,
		URLTemplate: "wss://host/ws/{call_id}",
		Codec:       smartfloCodecPCM16LE8K,
		AuthHeader:  "Authorization",
		MaxQueue:    8,
	}, map[string]string{})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "wss://host/ws/call-1", cfg.URLTemplate)
	require.Equal(t, 8000, cfg.SampleRate)
}

func TestResolveSmartfloWSConfigDisabledByFeatureFlag(t *testing.T) {
	cfg, err := resolveSmartfloWSConfig("call-1", 8000, &config.SmartfloWSConfig{
		Enabled:     true,
		URLTemplate: "wss://host/ws/{call_id}",
		Codec:       smartfloCodecPCM16LE8K,
	}, map[string]string{
		smartfloWSEnabledFeatureFlag: "false",
	})
	require.NoError(t, err)
	require.Nil(t, cfg)
}

func TestDecodeSmartfloMessageJSON(t *testing.T) {
	raw := pcm16ToBytesLE(msdk.PCM16Sample{1, -2, 3})
	msg, err := json.Marshal(map[string]string{
		"audio": base64.StdEncoding.EncodeToString(raw),
	})
	require.NoError(t, err)
	got, ok := decodeSmartfloMessage(websocket.TextMessage, msg)
	require.True(t, ok)
	require.Equal(t, msdk.PCM16Sample{1, -2, 3}, got)
}

func TestSmartfloWSBridgeLifecycle(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)
		defer conn.Close()
		for {
			typ, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if typ == websocket.BinaryMessage {
				_ = conn.WriteMessage(websocket.BinaryMessage, data)
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/stream/{call_id}"
	sink := &testPCMWriter{sampleRate: 8000}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bridge, err := newSmartfloWSBridge(ctx, logger.GetLogger(), "call-42", 8000, sink, &config.SmartfloWSConfig{
		Enabled:          true,
		URLTemplate:      wsURL,
		Codec:            smartfloCodecPCM16LE8K,
		HandshakeTimeout: 2 * time.Second,
		WriteTimeout:     2 * time.Second,
		ReadTimeout:      4 * time.Second,
		PingInterval:     0,
		MaxQueue:         16,
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, bridge)
	defer bridge.Close()

	next := &testPCMWriter{sampleRate: 8000}
	tap := bridge.Processor(next)
	require.NoError(t, tap.WriteSample(msdk.PCM16Sample{100, -100, 100}))

	require.Eventually(t, func() bool {
		got := sink.LastSample()
		return len(got) == 3 && got[0] == 100 && got[1] == -100 && got[2] == 100
	}, 3*time.Second, 20*time.Millisecond)
}
