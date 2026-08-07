package sip

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	msdk "github.com/livekit/media-sdk"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/sip/pkg/config"
)

const (
	smartfloCodecPCM16LE8K  = "pcm16le_8k"
	smartfloCodecPCM16LE16K = "pcm16le_16k"
)

type smartfloWSBridge struct {
	log             logger.Logger
	callID          string
	url             string
	sampleRateIn    int
	sampleRateSmart int
	sendTimeout     time.Duration
	readTimeout     time.Duration
	pingInterval    time.Duration
	queue           chan []byte
	conn            *websocket.Conn
	recvWriter      msdk.PCM16Writer

	closeOnce sync.Once
	closed    chan struct{}
	wg        sync.WaitGroup
	writeMu   sync.Mutex

	txFrames atomic.Uint64
	rxFrames atomic.Uint64
	dropped  atomic.Uint64
	errors   atomic.Uint64
}

func newSmartfloWSBridge(
	ctx context.Context,
	log logger.Logger,
	callID string,
	sampleRateIn int,
	sipOut msdk.PCM16Writer,
	base *config.SmartfloWSConfig,
	featureFlags map[string]string,
) (*smartfloWSBridge, error) {
	conf, err := resolveSmartfloWSConfig(callID, sampleRateIn, base, featureFlags)
	if err != nil {
		return nil, err
	}
	if conf == nil {
		return nil, nil
	}
	if sipOut == nil {
		return nil, errors.New("smartflo ws requires sip output writer")
	}

	headers := http.Header{}
	for k, v := range conf.Headers {
		headers.Set(k, v)
	}
	if conf.AuthToken != "" {
		headers.Set(conf.AuthHeader, conf.AuthToken)
	}
	dialer := websocket.Dialer{
		HandshakeTimeout: conf.HandshakeTimeout,
	}
	conn, _, err := dialer.DialContext(ctx, conf.URLTemplate, headers)
	if err != nil {
		return nil, fmt.Errorf("smartflo ws dial failed: %w", err)
	}

	recvWriter := msdk.ResampleWriter(sipOut, conf.SampleRate)
	b := &smartfloWSBridge{
		log:             log.WithValues("smartfloWS", true, "smartfloURL", conf.URLTemplate),
		callID:          callID,
		url:             conf.URLTemplate,
		sampleRateIn:    sampleRateIn,
		sampleRateSmart: conf.SampleRate,
		sendTimeout:     conf.WriteTimeout,
		readTimeout:     conf.ReadTimeout,
		pingInterval:    conf.PingInterval,
		queue:           make(chan []byte, conf.MaxQueue),
		conn:            conn,
		recvWriter:      recvWriter,
		closed:          make(chan struct{}),
	}
	if b.readTimeout > 0 {
		_ = b.conn.SetReadDeadline(time.Now().Add(b.readTimeout))
	}
	if b.pingInterval > 0 {
		b.conn.SetPongHandler(func(string) error {
			if b.readTimeout > 0 {
				return b.conn.SetReadDeadline(time.Now().Add(b.readTimeout))
			}
			return nil
		})
	}

	b.wg.Add(1)
	go b.writeLoop()
	b.wg.Add(1)
	go b.readLoop()
	if b.pingInterval > 0 {
		b.wg.Add(1)
		go b.pingLoop()
	}
	b.log.Infow("smartflo websocket connected", "callID", b.callID, "sampleRateIn", b.sampleRateIn, "sampleRateSmartflo", b.sampleRateSmart)
	return b, nil
}

func (b *smartfloWSBridge) closeWithReason(reason string) {
	b.closeOnce.Do(func() {
		close(b.closed)
		b.writeMu.Lock()
		_ = b.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, reason), time.Now().Add(time.Second))
		b.writeMu.Unlock()
		_ = b.conn.Close()
		b.wg.Wait()
		if b.recvWriter != nil {
			_ = b.recvWriter.Close()
		}
		b.log.Infow("smartflo websocket closed", "callID", b.callID, "txFrames", b.txFrames.Load(), "rxFrames", b.rxFrames.Load(), "droppedFrames", b.dropped.Load(), "errors", b.errors.Load())
	})
}

func (b *smartfloWSBridge) Close() error {
	b.closeWithReason("call closed")
	return nil
}

func (b *smartfloWSBridge) Processor(next msdk.PCM16Writer) msdk.PCM16Writer {
	return &smartfloTapWriter{
		next:             next,
		bridge:           b,
		sampleRateIn:     b.sampleRateIn,
		sampleRateSmart:  b.sampleRateSmart,
		scratchResampled: make(msdk.PCM16Sample, 0),
	}
}

func (b *smartfloWSBridge) enqueueSample(sample msdk.PCM16Sample) {
	payload := pcm16ToBytesLE(sample)
	select {
	case b.queue <- payload:
	default:
		b.dropped.Add(1)
	}
}

func (b *smartfloWSBridge) writeLoop() {
	defer b.wg.Done()
	for {
		select {
		case <-b.closed:
			return
		case payload := <-b.queue:
			if b.sendTimeout > 0 {
				_ = b.conn.SetWriteDeadline(time.Now().Add(b.sendTimeout))
			}
			b.writeMu.Lock()
			err := b.conn.WriteMessage(websocket.BinaryMessage, payload)
			b.writeMu.Unlock()
			if err != nil {
				b.errors.Add(1)
				b.log.Warnw("smartflo websocket write failed", err, "callID", b.callID)
				b.closeWithReason("write failure")
				return
			}
			b.txFrames.Add(1)
		}
	}
}

func (b *smartfloWSBridge) pingLoop() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.closed:
			return
		case <-ticker.C:
			if b.sendTimeout > 0 {
				_ = b.conn.SetWriteDeadline(time.Now().Add(b.sendTimeout))
			}
			b.writeMu.Lock()
			err := b.conn.WriteMessage(websocket.PingMessage, nil)
			b.writeMu.Unlock()
			if err != nil {
				b.errors.Add(1)
				b.log.Warnw("smartflo websocket ping failed", err, "callID", b.callID)
				b.closeWithReason("ping failure")
				return
			}
		}
	}
}

func (b *smartfloWSBridge) readLoop() {
	defer b.wg.Done()
	for {
		select {
		case <-b.closed:
			return
		default:
		}
		typ, data, err := b.conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				b.closeWithReason("peer closed")
				return
			}
			b.errors.Add(1)
			b.log.Warnw("smartflo websocket read failed", err, "callID", b.callID)
			b.closeWithReason("read failure")
			return
		}
		if b.readTimeout > 0 {
			_ = b.conn.SetReadDeadline(time.Now().Add(b.readTimeout))
		}
		pcm, ok := decodeSmartfloMessage(typ, data)
		if !ok || len(pcm) == 0 {
			continue
		}
		if err := b.recvWriter.WriteSample(pcm); err != nil {
			b.errors.Add(1)
			b.log.Warnw("smartflo websocket audio write failed", err, "callID", b.callID)
			continue
		}
		b.rxFrames.Add(1)
	}
}

type smartfloTapWriter struct {
	next             msdk.PCM16Writer
	bridge           *smartfloWSBridge
	sampleRateIn     int
	sampleRateSmart  int
	scratchResampled msdk.PCM16Sample
}

func (w *smartfloTapWriter) String() string {
	return fmt.Sprintf("SmartfloTap(%d->%d) -> %s", w.sampleRateIn, w.sampleRateSmart, w.next.String())
}

func (w *smartfloTapWriter) SampleRate() int {
	return w.next.SampleRate()
}

func (w *smartfloTapWriter) Close() error {
	return w.next.Close()
}

func (w *smartfloTapWriter) WriteSample(sample msdk.PCM16Sample) error {
	if err := w.next.WriteSample(sample); err != nil {
		return err
	}
	if w.bridge == nil {
		return nil
	}
	toSend := sample
	if w.sampleRateIn > 0 && w.sampleRateSmart > 0 && w.sampleRateIn != w.sampleRateSmart {
		toSend = msdk.Resample(w.scratchResampled[:0], w.sampleRateSmart, sample, w.sampleRateIn)
	}
	w.bridge.enqueueSample(toSend)
	return nil
}

type smartfloWSResolvedConfig struct {
	URLTemplate      string
	AuthToken        string
	AuthHeader       string
	Headers          map[string]string
	HandshakeTimeout time.Duration
	WriteTimeout     time.Duration
	ReadTimeout      time.Duration
	PingInterval     time.Duration
	SampleRate       int
	MaxQueue         int
}

func resolveSmartfloWSConfig(callID string, sampleRateIn int, base *config.SmartfloWSConfig, featureFlags map[string]string) (*smartfloWSResolvedConfig, error) {
	if base == nil {
		return nil, nil
	}
	enabled := base.Enabled
	if raw, ok := featureFlags[smartfloWSEnabledFeatureFlag]; ok && raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid %s flag: %w", smartfloWSEnabledFeatureFlag, err)
		}
		enabled = v
	}
	if !enabled {
		return nil, nil
	}

	url := base.URLTemplate
	if v := featureFlags[smartfloWSURLFeatureFlag]; v != "" {
		url = v
	}
	if url == "" {
		return nil, errors.New("smartflo ws enabled but url_template is empty")
	}
	url = strings.ReplaceAll(url, "{call_id}", callID)
	url = strings.ReplaceAll(url, "${call_id}", callID)

	token := base.AuthToken
	if v := featureFlags[smartfloWSTokenFeatureFlag]; v != "" {
		token = v
	}
	codec := strings.TrimSpace(strings.ToLower(base.Codec))
	if v := featureFlags[smartfloWSCodecFeatureFlag]; v != "" {
		codec = strings.TrimSpace(strings.ToLower(v))
	}
	if codec == "" {
		switch sampleRateIn {
		case 8000:
			codec = smartfloCodecPCM16LE8K
		case 16000:
			codec = smartfloCodecPCM16LE16K
		default:
			return nil, fmt.Errorf("unsupported smartflo codec auto-detect for input sample rate %d", sampleRateIn)
		}
	}
	smartRate, err := smartfloCodecSampleRate(codec)
	if err != nil {
		return nil, err
	}
	if sampleRateIn <= 0 {
		return nil, errors.New("invalid input sample rate for smartflo ws")
	}

	headers := map[string]string{}
	for k, v := range base.Headers {
		headers[k] = v
	}
	authHeader := base.AuthHeader
	if authHeader == "" {
		authHeader = "Authorization"
	}
	return &smartfloWSResolvedConfig{
		URLTemplate:      url,
		AuthToken:        token,
		AuthHeader:       authHeader,
		Headers:          headers,
		HandshakeTimeout: base.HandshakeTimeout,
		WriteTimeout:     base.WriteTimeout,
		ReadTimeout:      base.ReadTimeout,
		PingInterval:     base.PingInterval,
		SampleRate:       smartRate,
		MaxQueue:         base.MaxQueue,
	}, nil
}

func smartfloCodecSampleRate(codec string) (int, error) {
	switch strings.TrimSpace(strings.ToLower(codec)) {
	case smartfloCodecPCM16LE8K:
		return 8000, nil
	case smartfloCodecPCM16LE16K:
		return 16000, nil
	default:
		return 0, fmt.Errorf("unsupported smartflo ws codec: %q", codec)
	}
}

func pcm16ToBytesLE(sample msdk.PCM16Sample) []byte {
	buf := make([]byte, len(sample)*2)
	for i, s := range sample {
		binary.LittleEndian.PutUint16(buf[i*2:i*2+2], uint16(s))
	}
	return buf
}

func bytesToPCM16LE(data []byte) msdk.PCM16Sample {
	if len(data)%2 != 0 {
		data = data[:len(data)-1]
	}
	out := make(msdk.PCM16Sample, len(data)/2)
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(data[i*2 : i*2+2]))
	}
	return out
}

func decodeSmartfloMessage(messageType int, data []byte) (msdk.PCM16Sample, bool) {
	switch messageType {
	case websocket.BinaryMessage:
		return bytesToPCM16LE(data), true
	case websocket.TextMessage:
		var payload struct {
			Audio   string `json:"audio"`
			Payload string `json:"payload"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, false
		}
		encoded := payload.Audio
		if encoded == "" {
			encoded = payload.Payload
		}
		if encoded == "" {
			return nil, false
		}
		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, false
		}
		return bytesToPCM16LE(raw), true
	default:
		return nil, false
	}
}

func withSmartfloProcessor(base msdk.PCM16Processor, b *smartfloWSBridge) msdk.PCM16Processor {
	if b == nil {
		return base
	}
	return func(next msdk.PCM16Writer) msdk.PCM16Writer {
		if base != nil {
			next = base(next)
		}
		return b.Processor(next)
	}
}
