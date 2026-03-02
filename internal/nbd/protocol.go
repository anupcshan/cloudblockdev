// Package nbd implements the NBD (Network Block Device) protocol.
package nbd

import (
	"encoding/binary"
	"io"
)

// NBD protocol constants
const (
	// Magic numbers
	NBDMagic        = 0x4e42444d41474943 // "NBDMAGIC"
	NBDOptionMagic  = 0x49484156454F5054 // "IHAVEOPT"
	NBDReplyMagic   = 0x3e889045565a9
	NBDRequestMagic = 0x25609513
	NBDSimpleReply  = 0x67446698

	// Handshake flags (server)
	NBDFlagFixedNewstyle = 1 << 0
	NBDFlagNoZeroes      = 1 << 1

	// Client flags
	NBDFlagCFixedNewstyle = 1 << 0
	NBDFlagCNoZeroes      = 1 << 1

	// Transmission flags
	NBDFlagHasFlags   = 1 << 0
	NBDFlagReadOnly   = 1 << 1
	NBDFlagSendFlush  = 1 << 2
	NBDFlagSendFUA    = 1 << 3
	NBDFlagRotational = 1 << 4
	NBDFlagSendTrim   = 1 << 5

	// Option types
	NBDOptExportName = 1
	NBDOptAbort      = 2
	NBDOptList       = 3
	NBDOptInfo       = 6
	NBDOptGo         = 7

	// Option reply types
	NBDRepAck         = 1
	NBDRepServer      = 2
	NBDRepInfo        = 3
	NBDRepErrUnsup    = (1 << 31) + 1
	NBDRepErrPolicy   = (1 << 31) + 2
	NBDRepErrInvalid  = (1 << 31) + 3
	NBDRepErrPlatform = (1 << 31) + 4
	NBDRepErrTlsReqd  = (1 << 31) + 5
	NBDRepErrUnknown  = (1 << 31) + 6

	// Info types (for NBD_OPT_INFO and NBD_OPT_GO)
	NBDInfoExport    = 0
	NBDInfoName      = 1
	NBDInfoDesc      = 2
	NBDInfoBlockSize = 3

	// Command flags
	NBDCmdFlagFUA = 1 << 0 // Force Unit Access - write must be durable before reply

	// Command types
	NBDCmdRead       = 0
	NBDCmdWrite      = 1
	NBDCmdDisc       = 2
	NBDCmdFlush      = 3
	NBDCmdTrim       = 4
	NBDCmdWriteZeros = 6

	// Error codes
	NBDErrNone     = 0
	NBDErrPerm     = 1
	NBDErrIO       = 5
	NBDErrNomem    = 12
	NBDErrInval    = 22
	NBDErrNospc    = 28
	NBDErrOverflow = 75
	NBDErrShutdown = 108
)

// Request represents an NBD request from the client.
type Request struct {
	Magic  uint32
	Flags  uint16
	Type   uint16
	Handle uint64
	Offset uint64
	Length uint32
}

// ReadRequest reads an NBD request from the connection.
func ReadRequest(r io.Reader) (*Request, error) {
	req := &Request{}
	if err := binary.Read(r, binary.BigEndian, req); err != nil {
		return nil, err
	}
	return req, nil
}

// SimpleReply represents an NBD simple reply to the client.
type SimpleReply struct {
	Magic  uint32
	Error  uint32
	Handle uint64
}

// WriteSimpleReply writes a simple reply to the connection.
func WriteSimpleReply(w io.Writer, handle uint64, errCode uint32) error {
	reply := SimpleReply{
		Magic:  NBDSimpleReply,
		Error:  errCode,
		Handle: handle,
	}
	return binary.Write(w, binary.BigEndian, &reply)
}

// OptionRequest represents an NBD option request during negotiation.
type OptionRequest struct {
	Magic  uint64
	Option uint32
	Length uint32
}

// ReadOptionRequest reads an option request from the connection.
func ReadOptionRequest(r io.Reader) (*OptionRequest, error) {
	req := &OptionRequest{}
	if err := binary.Read(r, binary.BigEndian, req); err != nil {
		return nil, err
	}
	return req, nil
}

// OptionReply represents an NBD option reply during negotiation.
type OptionReply struct {
	Magic  uint64
	Option uint32
	Type   uint32
	Length uint32
}

// WriteOptionReply writes an option reply to the connection.
func WriteOptionReply(w io.Writer, option, replyType, length uint32) error {
	reply := OptionReply{
		Magic:  NBDReplyMagic,
		Option: option,
		Type:   replyType,
		Length: length,
	}
	return binary.Write(w, binary.BigEndian, &reply)
}
