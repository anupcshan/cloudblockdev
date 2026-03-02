// Package nbdclient provides a client for managing kernel NBD devices
// via the netlink interface.
package nbdclient

import (
	"fmt"

	"github.com/mdlayher/genetlink"
	"github.com/mdlayher/netlink"
)

// NBD generic netlink family name
const nbdGenlFamilyName = "nbd"

// NBD netlink commands (from linux/nbd-netlink.h)
const (
	nbdCmdUnspec      = 0
	nbdCmdConnect     = 1
	nbdCmdDisconnect  = 2
	nbdCmdReconfigure = 3
	nbdCmdLinkDead    = 4
	nbdCmdStatus      = 5
)

// NBD netlink attributes (from linux/nbd-netlink.h)
const (
	nbdAttrUnspec            = 0
	nbdAttrIndex             = 1
	nbdAttrSizeBytes         = 2
	nbdAttrBlockSizeBytes    = 3
	nbdAttrTimeout           = 4
	nbdAttrServerFlags       = 5
	nbdAttrClientFlags       = 6
	nbdAttrSockets           = 7
	nbdAttrDeadConnTimeout   = 8
	nbdAttrDeviceList        = 9
	nbdAttrBackendIdentifier = 10
)

// NBD socket attributes (nested within nbdAttrSockets)
const (
	nbdSockItemUnspec = 0
	nbdSockItem       = 1
)

const (
	nbdSockUnspec = 0
	nbdSockFD     = 1
)

// NBD server flags (from linux/nbd.h)
const (
	NBDFlagHasFlags        = 1 << 0
	NBDFlagReadOnly        = 1 << 1
	NBDFlagSendFlush       = 1 << 2
	NBDFlagSendFUA         = 1 << 3
	NBDFlagRotational      = 1 << 4
	NBDFlagSendTrim        = 1 << 5
	NBDFlagSendWriteZeroes = 1 << 6
)

// Client is a netlink client for managing NBD devices.
type Client struct {
	conn   *genetlink.Conn
	family genetlink.Family
}

// NewClient creates a new NBD netlink client.
func NewClient() (*Client, error) {
	conn, err := genetlink.Dial(nil)
	if err != nil {
		return nil, fmt.Errorf("dial genetlink: %w", err)
	}

	family, err := conn.GetFamily(nbdGenlFamilyName)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("get NBD family (is the nbd module loaded?): %w", err)
	}

	return &Client{
		conn:   conn,
		family: family,
	}, nil
}

// Close closes the netlink connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// ConnectConfig contains configuration for connecting an NBD device.
type ConnectConfig struct {
	Index           int    // Device index (-1 for auto-assign)
	SockFD          int    // Socket file descriptor
	SizeBytes       uint64 // Device size in bytes
	BlockSize       uint32 // Block size (typically 4096)
	Timeout         uint64 // I/O timeout in seconds (0 = no timeout)
	DeadConnTimeout uint64 // Dead connection timeout in seconds
	ServerFlags     uint64 // Server capability flags
}

// Connect creates and configures a new NBD device.
// Returns the device index on success.
func (c *Client) Connect(cfg ConnectConfig) (int, error) {
	ae := netlink.NewAttributeEncoder()

	if cfg.Index >= 0 {
		ae.Uint32(nbdAttrIndex, uint32(cfg.Index))
	}
	ae.Uint64(nbdAttrSizeBytes, cfg.SizeBytes)
	ae.Uint64(nbdAttrBlockSizeBytes, uint64(cfg.BlockSize))
	if cfg.Timeout > 0 {
		ae.Uint64(nbdAttrTimeout, cfg.Timeout)
	}
	if cfg.DeadConnTimeout > 0 {
		ae.Uint64(nbdAttrDeadConnTimeout, cfg.DeadConnTimeout)
	}
	ae.Uint64(nbdAttrServerFlags, cfg.ServerFlags)

	// Encode the socket FD
	ae.Nested(nbdAttrSockets, func(nae *netlink.AttributeEncoder) error {
		nae.Nested(nbdSockItem, func(sae *netlink.AttributeEncoder) error {
			sae.Uint32(nbdSockFD, uint32(cfg.SockFD))
			return nil
		})
		return nil
	})

	attrs, err := ae.Encode()
	if err != nil {
		return -1, fmt.Errorf("encode attributes: %w", err)
	}

	msg := genetlink.Message{
		Header: genetlink.Header{
			Command: nbdCmdConnect,
			Version: 1,
		},
		Data: attrs,
	}

	msgs, err := c.conn.Execute(msg, c.family.ID, netlink.Request|netlink.Acknowledge)
	if err != nil {
		return -1, fmt.Errorf("execute connect: %w", err)
	}

	// Parse response to get the assigned index
	if len(msgs) > 0 {
		ad, err := netlink.NewAttributeDecoder(msgs[0].Data)
		if err != nil {
			return -1, fmt.Errorf("decode response: %w", err)
		}

		for ad.Next() {
			if ad.Type() == nbdAttrIndex {
				return int(ad.Uint32()), nil
			}
		}
	}

	// If index was specified, return it
	if cfg.Index >= 0 {
		return cfg.Index, nil
	}

	return -1, fmt.Errorf("no index returned from kernel")
}

// Reconfigure updates an existing NBD device with a new socket.
// This is used to reconnect after a restart.
func (c *Client) Reconfigure(index int, sockFD int) error {
	ae := netlink.NewAttributeEncoder()
	ae.Uint32(nbdAttrIndex, uint32(index))

	// Encode the socket FD
	ae.Nested(nbdAttrSockets, func(nae *netlink.AttributeEncoder) error {
		nae.Nested(nbdSockItem, func(sae *netlink.AttributeEncoder) error {
			sae.Uint32(nbdSockFD, uint32(sockFD))
			return nil
		})
		return nil
	})

	attrs, err := ae.Encode()
	if err != nil {
		return fmt.Errorf("encode attributes: %w", err)
	}

	msg := genetlink.Message{
		Header: genetlink.Header{
			Command: nbdCmdReconfigure,
			Version: 1,
		},
		Data: attrs,
	}

	_, err = c.conn.Execute(msg, c.family.ID, netlink.Request|netlink.Acknowledge)
	if err != nil {
		return fmt.Errorf("execute reconfigure: %w", err)
	}

	return nil
}

// Disconnect tears down an NBD device.
func (c *Client) Disconnect(index int) error {
	ae := netlink.NewAttributeEncoder()
	ae.Uint32(nbdAttrIndex, uint32(index))

	attrs, err := ae.Encode()
	if err != nil {
		return fmt.Errorf("encode attributes: %w", err)
	}

	msg := genetlink.Message{
		Header: genetlink.Header{
			Command: nbdCmdDisconnect,
			Version: 1,
		},
		Data: attrs,
	}

	_, err = c.conn.Execute(msg, c.family.ID, netlink.Request|netlink.Acknowledge)
	if err != nil {
		return fmt.Errorf("execute disconnect: %w", err)
	}

	return nil
}

// DeviceStatus contains status information for an NBD device.
type DeviceStatus struct {
	Index     int
	Connected bool
}

// Status queries the status of an NBD device.
func (c *Client) Status(index int) (*DeviceStatus, error) {
	ae := netlink.NewAttributeEncoder()
	ae.Uint32(nbdAttrIndex, uint32(index))

	attrs, err := ae.Encode()
	if err != nil {
		return nil, fmt.Errorf("encode attributes: %w", err)
	}

	msg := genetlink.Message{
		Header: genetlink.Header{
			Command: nbdCmdStatus,
			Version: 1,
		},
		Data: attrs,
	}

	msgs, err := c.conn.Execute(msg, c.family.ID, netlink.Request|netlink.Acknowledge)
	if err != nil {
		return nil, fmt.Errorf("execute status: %w", err)
	}

	status := &DeviceStatus{Index: index}

	if len(msgs) > 0 {
		ad, err := netlink.NewAttributeDecoder(msgs[0].Data)
		if err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}

		for ad.Next() {
			// Parse device list to find our device
			// The structure is: NBD_ATTR_DEVICE_LIST > NBD_DEVICE_ITEM > attributes
			if ad.Type() == nbdAttrDeviceList {
				ad.Nested(func(dad *netlink.AttributeDecoder) error {
					for dad.Next() {
						// Each item in the list
						dad.Nested(func(iad *netlink.AttributeDecoder) error {
							for iad.Next() {
								switch iad.Type() {
								case 1: // NBD_DEVICE_INDEX
									if int(iad.Uint32()) == index {
										status.Connected = true
									}
								}
							}
							return nil
						})
					}
					return nil
				})
			}
		}
	}

	return status, nil
}
