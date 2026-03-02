package nbdclient

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// DeviceConfig contains configuration for the NBD device manager.
type DeviceConfig struct {
	Index           int           // Device index (0 = /dev/nbd0, -1 for auto-assign)
	Size            uint64        // Device size in bytes
	BlockSize       uint32        // Block size (default 4096)
	DeadConnTimeout time.Duration // How long kernel waits for reconnect
	ServerFlags     uint64        // NBD server flags to advertise
	ForceReconnect  bool          // Disconnect existing device before connecting
}

// DeviceManager manages a kernel NBD device using the netlink interface.
type DeviceManager struct {
	cfg        DeviceConfig
	client     *Client
	index      int      // Actual device index (after Connect)
	serverConn net.Conn // Our end of the socketpair (for NBD server)
	kernelFD   int      // Kernel's end (passed to netlink)
	connected  bool
}

// NewDeviceManager creates a new device manager.
func NewDeviceManager(cfg DeviceConfig) *DeviceManager {
	if cfg.BlockSize == 0 {
		cfg.BlockSize = 4096
	}
	if cfg.DeadConnTimeout == 0 {
		cfg.DeadConnTimeout = 120 * time.Second
	}
	// Default server flags: support flush, FUA, and trim
	if cfg.ServerFlags == 0 {
		cfg.ServerFlags = NBDFlagHasFlags | NBDFlagSendFlush | NBDFlagSendFUA | NBDFlagSendTrim
	}

	return &DeviceManager{
		cfg:      cfg,
		index:    cfg.Index,
		kernelFD: -1,
	}
}

// Setup initializes the NBD device.
// On first run, it creates the device with NBD_CMD_CONNECT.
// On restart (device exists with dead socket), it uses NBD_CMD_RECONFIGURE.
func (m *DeviceManager) Setup() error {
	// Create netlink client
	client, err := NewClient()
	if err != nil {
		return fmt.Errorf("create netlink client: %w", err)
	}
	m.client = client

	// Create socketpair for communication between our NBD server and kernel
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		m.cleanup()
		return fmt.Errorf("create socketpair: %w", err)
	}

	// fds[0] = our end (for NBD server)
	// fds[1] = kernel's end (passed to netlink)
	m.kernelFD = fds[1]

	// Wrap our end as a net.Conn
	ourFile := os.NewFile(uintptr(fds[0]), "nbd-server")
	serverConn, err := net.FileConn(ourFile)
	ourFile.Close() // FileConn dups the fd, so close the original
	if err != nil {
		syscall.Close(fds[1])
		m.kernelFD = -1
		m.cleanup()
		return fmt.Errorf("create server conn: %w", err)
	}
	m.serverConn = serverConn

	// Track if we did a force disconnect (need retry logic for connect)
	didForceDisconnect := false

	// Check if device already exists (restart case)
	if m.cfg.Index >= 0 && m.isDeviceConfigured() {
		if m.cfg.ForceReconnect {
			// Force mode: disconnect first, then do fresh connect
			log.Printf("Force reconnect: disconnecting existing /dev/nbd%d", m.cfg.Index)
			if err := m.client.Disconnect(m.cfg.Index); err != nil {
				// Log but continue - device might already be in a bad state
				log.Printf("Warning: disconnect failed (continuing anyway): %v", err)
			}
			didForceDisconnect = true
			// Fall through to fresh connect below
		} else {
			// Normal restart: verify size matches
			kernelSize, err := m.getKernelDeviceSize()
			if err != nil {
				m.cleanup()
				return fmt.Errorf("get kernel device size: %w", err)
			}
			if kernelSize != m.cfg.Size {
				m.cleanup()
				return fmt.Errorf("device size mismatch: kernel has %d bytes, config has %d bytes. "+
					"Use -force-reconnect to disconnect and reconnect with new size",
					kernelSize, m.cfg.Size)
			}

			// Device exists with matching size, reconfigure with new socket
			err = m.client.Reconfigure(m.cfg.Index, m.kernelFD)
			if err != nil {
				m.cleanup()
				return fmt.Errorf("reconfigure device: %w", err)
			}
			m.index = m.cfg.Index
			m.connected = true
			return nil
		}
	}

	// First time setup - create new device
	connectCfg := ConnectConfig{
		Index:           m.cfg.Index,
		SockFD:          m.kernelFD,
		SizeBytes:       m.cfg.Size,
		BlockSize:       m.cfg.BlockSize,
		Timeout:         0, // No I/O timeout (let ZFS handle retries)
		DeadConnTimeout: uint64(m.cfg.DeadConnTimeout.Seconds()),
		ServerFlags:     m.cfg.ServerFlags,
	}

	// If we just did a force disconnect, the kernel may need time to fully release
	// the device. Retry with backoff.
	var index int
	if didForceDisconnect {
		maxRetries := 10
		baseDelay := 100 * time.Millisecond
		for i := 0; i < maxRetries; i++ {
			index, err = m.client.Connect(connectCfg)
			if err == nil {
				break
			}
			if i < maxRetries-1 {
				delay := baseDelay * time.Duration(1<<i) // Exponential backoff
				if delay > 2*time.Second {
					delay = 2 * time.Second
				}
				log.Printf("Connect attempt %d failed (retrying in %v): %v", i+1, delay, err)
				time.Sleep(delay)
			}
		}
	} else {
		index, err = m.client.Connect(connectCfg)
	}

	if err != nil {
		m.cleanup()
		return fmt.Errorf("connect device: %w", err)
	}

	m.index = index
	m.connected = true
	return nil
}

// isDeviceConfigured checks if the NBD device is already set up in the kernel.
// This is used to detect restart scenarios.
func (m *DeviceManager) isDeviceConfigured() bool {
	// Check if /sys/block/nbdX/pid exists and has content
	// A configured device will have a pid file even if the connection is dead
	pidPath := fmt.Sprintf("/sys/block/nbd%d/pid", m.cfg.Index)
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return false
	}
	// If pid file exists and has content (even "0" or empty after disconnect),
	// the device is configured
	return len(data) > 0
}

// getKernelDeviceSize returns the current size of the NBD device as known by the kernel.
// The size is read from /sys/block/nbdX/size (in 512-byte sectors) and converted to bytes.
func (m *DeviceManager) getKernelDeviceSize() (uint64, error) {
	sizePath := fmt.Sprintf("/sys/block/nbd%d/size", m.cfg.Index)
	data, err := os.ReadFile(sizePath)
	if err != nil {
		return 0, fmt.Errorf("read device size: %w", err)
	}
	sectors, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse device size: %w", err)
	}
	return sectors * 512, nil
}

// ServerConn returns the server's end of the socketpair.
// This connection should be passed to the NBD server to handle protocol messages.
func (m *DeviceManager) ServerConn() net.Conn {
	return m.serverConn
}

// Index returns the device index (e.g., 0 for /dev/nbd0).
func (m *DeviceManager) Index() int {
	return m.index
}

// DevicePath returns the device path (e.g., /dev/nbd0).
func (m *DeviceManager) DevicePath() string {
	return fmt.Sprintf("/dev/nbd%d", m.index)
}

// Disconnect tears down the NBD device.
// This should only be called when you want to completely remove the device,
// not during normal restarts (let the kernel wait for reconnect instead).
func (m *DeviceManager) Disconnect() error {
	if !m.connected {
		return nil
	}

	if m.client != nil {
		err := m.client.Disconnect(m.index)
		if err != nil {
			return fmt.Errorf("disconnect: %w", err)
		}
	}

	m.cleanup()
	m.connected = false
	return nil
}

// Close cleans up resources without disconnecting the kernel device.
// The device will remain and wait for reconnect (up to DeadConnTimeout).
func (m *DeviceManager) Close() error {
	// Close server connection - this will signal the kernel that socket is dead
	if m.serverConn != nil {
		m.serverConn.Close()
		m.serverConn = nil
	}

	// Close kernel FD if we still have it
	if m.kernelFD >= 0 {
		syscall.Close(m.kernelFD)
		m.kernelFD = -1
	}

	// Close netlink client
	if m.client != nil {
		m.client.Close()
		m.client = nil
	}

	return nil
}

func (m *DeviceManager) cleanup() {
	if m.serverConn != nil {
		m.serverConn.Close()
		m.serverConn = nil
	}

	if m.kernelFD >= 0 {
		syscall.Close(m.kernelFD)
		m.kernelFD = -1
	}

	if m.client != nil {
		m.client.Close()
		m.client = nil
	}
}
