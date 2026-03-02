package nbd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// OpResult is the result of an async operation.
type OpResult struct {
	Data []byte
	N    int
	Err  error
}

// BlockDevice is the interface for block devices that can be exported via NBD.
// All operations are asynchronous - they return a channel that will receive the result.
type BlockDevice interface {
	Size() int64

	// SubmitRead queues a read operation.
	// handle is the NBD request handle for reply correlation.
	// Returns a channel that will receive the result (with Data filled).
	SubmitRead(handle uint64, length int, offset int64) <-chan OpResult

	// SubmitWrite queues a write operation.
	// Returns a channel that will receive the result.
	SubmitWrite(handle uint64, data []byte, offset int64) <-chan OpResult

	// SubmitWriteFUA queues a write with Force Unit Access.
	// The write must be durable (uploaded to S3) before the result is sent.
	// Unlike Flush, this only waits for THIS write - not prior pending writes.
	SubmitWriteFUA(handle uint64, data []byte, offset int64) <-chan OpResult

	// SubmitFlush queues a flush operation.
	// Flush acts as a barrier - waits for prior writes to be persisted.
	SubmitFlush(handle uint64) <-chan OpResult

	// SubmitTrim queues a trim/discard operation.
	SubmitTrim(handle uint64, offset, length int64) <-chan OpResult

	// SubmitTrimFUA queues a trim with Force Unit Access (for write-zeroes with FUA).
	// The trim must be durable before the result is sent.
	SubmitTrimFUA(handle uint64, offset, length int64) <-chan OpResult

	// Start begins background processing. Must be called before submitting operations.
	Start() error

	// Stop gracefully shuts down, waiting for pending operations.
	Stop(timeout time.Duration) error
}

// Server represents an NBD server.
type Server struct {
	device     BlockDevice
	exportName string
	onFlush    func() // called after flush completes (for async S3 upload)
	listener net.Listener

	mu          sync.Mutex
	shutdown    bool
	conn        net.Conn   // current TCP connection (single client)
	directConns []net.Conn // kernel direct connections (multi-connection support)
}

// Config holds configuration for creating a Server.
type Config struct {
	// Device is the block device to export.
	Device BlockDevice
	// ExportName is the name of the export (can be empty for default).
	ExportName string
	// OnFlush is called after a flush completes (can be nil).
	// Use this to trigger async S3 upload.
	OnFlush func()
}

// NewServer creates a new NBD server.
func NewServer(cfg Config) *Server {
	return &Server{
		device:     cfg.Device,
		exportName: cfg.ExportName,
		onFlush:    cfg.OnFlush,
	}
}

// Serve starts the NBD server on the given listener.
func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	log.Printf("NBD server listening on %s", listener.Addr())

	for {
		conn, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			shutdown := s.shutdown
			s.mu.Unlock()
			if shutdown {
				return nil
			}
			return fmt.Errorf("accept: %w", err)
		}

		// Single client only - reject if we have an active connection
		s.mu.Lock()
		if s.conn != nil {
			s.mu.Unlock()
			log.Printf("Rejecting connection from %s: already have a client", conn.RemoteAddr())
			conn.Close()
			continue
		}
		s.conn = conn
		s.mu.Unlock()

		go s.handleConnection(conn)
	}
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown() error {
	s.mu.Lock()
	s.shutdown = true
	if s.listener != nil {
		s.listener.Close()
	}
	if s.conn != nil {
		s.conn.Close()
	}
	// Close all direct (kernel) connections
	for _, conn := range s.directConns {
		conn.Close()
	}
	s.mu.Unlock()
	return nil
}

// HandleConnection handles a single client connection with negotiation.
// This is the exported version for use with pre-connected sockets.
func (s *Server) HandleConnection(conn net.Conn) error {
	s.mu.Lock()
	if s.conn != nil {
		s.mu.Unlock()
		return fmt.Errorf("already have a connection")
	}
	s.conn = conn
	s.mu.Unlock()

	s.handleConnection(conn)
	return nil
}

// HandleDirectConnection handles a kernel NBD connection that was set up via netlink.
// This skips negotiation since the kernel already knows device parameters.
// Multiple connections are supported for kernel multi-connection mode.
func (s *Server) HandleDirectConnection(conn net.Conn) error {
	s.mu.Lock()
	s.directConns = append(s.directConns, conn)
	connIndex := len(s.directConns) - 1
	s.mu.Unlock()

	defer func() {
		conn.Close()
		s.mu.Lock()
		// Remove from directConns slice
		for i, c := range s.directConns {
			if c == conn {
				s.directConns = append(s.directConns[:i], s.directConns[i+1:]...)
				break
			}
		}
		s.mu.Unlock()
		log.Printf("Kernel NBD connection %d closed", connIndex)
	}()

	log.Printf("Kernel NBD connection %d ready", connIndex)

	// Skip negotiation - kernel already configured via netlink
	// Go straight to command handling
	ctx := context.Background()
	if err := s.handleCommandsWithConnID(ctx, conn, connIndex); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			log.Printf("Connection %d command handling error: %v", connIndex, err)
			return err
		}
	}
	return nil
}

// handleConnection handles a single client connection.
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		s.conn = nil
		s.mu.Unlock()
		log.Printf("Client disconnected: %s", conn.RemoteAddr())
	}()

	log.Printf("Client connected: %s", conn.RemoteAddr())

	// Perform newstyle negotiation
	if err := s.negotiate(conn); err != nil {
		log.Printf("Negotiation failed: %v", err)
		return
	}

	// Handle commands
	ctx := context.Background()
	if err := s.handleCommands(ctx, conn); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			log.Printf("Command handling error: %v", err)
		}
	}
}

// negotiate performs the NBD newstyle negotiation handshake.
func (s *Server) negotiate(conn net.Conn) error {
	// Send initial server handshake
	// Magic + Option magic + Handshake flags
	if err := binary.Write(conn, binary.BigEndian, uint64(NBDMagic)); err != nil {
		return fmt.Errorf("write magic: %w", err)
	}
	if err := binary.Write(conn, binary.BigEndian, uint64(NBDOptionMagic)); err != nil {
		return fmt.Errorf("write option magic: %w", err)
	}
	// Server flags: fixed newstyle + no zeroes
	serverFlags := uint16(NBDFlagFixedNewstyle | NBDFlagNoZeroes)
	if err := binary.Write(conn, binary.BigEndian, serverFlags); err != nil {
		return fmt.Errorf("write server flags: %w", err)
	}

	// Read client flags
	var clientFlags uint32
	if err := binary.Read(conn, binary.BigEndian, &clientFlags); err != nil {
		return fmt.Errorf("read client flags: %w", err)
	}

	// Option haggling
	for {
		optReq, err := ReadOptionRequest(conn)
		if err != nil {
			return fmt.Errorf("read option request: %w", err)
		}

		if optReq.Magic != NBDOptionMagic {
			return fmt.Errorf("invalid option magic: %x", optReq.Magic)
		}

		switch optReq.Option {
		case NBDOptExportName:
			// Read export name
			exportName := make([]byte, optReq.Length)
			if _, err := io.ReadFull(conn, exportName); err != nil {
				return fmt.Errorf("read export name: %w", err)
			}

			// Send export info (no reply header for NBD_OPT_EXPORT_NAME)
			// Just send: size (8 bytes) + transmission flags (2 bytes) + zeroes (124 bytes, if not NO_ZEROES)
			if err := binary.Write(conn, binary.BigEndian, uint64(s.device.Size())); err != nil {
				return fmt.Errorf("write size: %w", err)
			}
			transFlags := uint16(NBDFlagHasFlags | NBDFlagSendFlush | NBDFlagSendTrim)
			if err := binary.Write(conn, binary.BigEndian, transFlags); err != nil {
				return fmt.Errorf("write transmission flags: %w", err)
			}
			// If client didn't set NO_ZEROES, send 124 zero bytes
			if clientFlags&NBDFlagCNoZeroes == 0 {
				zeroes := make([]byte, 124)
				if _, err := conn.Write(zeroes); err != nil {
					return fmt.Errorf("write zeroes: %w", err)
				}
			}
			log.Printf("Export negotiated: name=%q size=%d", string(exportName), s.device.Size())
			return nil

		case NBDOptGo, NBDOptInfo:
			// Read export name length and name
			var nameLen uint32
			if err := binary.Read(conn, binary.BigEndian, &nameLen); err != nil {
				return fmt.Errorf("read name length: %w", err)
			}
			exportName := make([]byte, nameLen)
			if _, err := io.ReadFull(conn, exportName); err != nil {
				return fmt.Errorf("read export name: %w", err)
			}
			// Read number of info requests
			var numInfoReqs uint16
			if err := binary.Read(conn, binary.BigEndian, &numInfoReqs); err != nil {
				return fmt.Errorf("read num info reqs: %w", err)
			}
			// Track requested info types
			wantBlockSize := false
			for range numInfoReqs {
				var infoType uint16
				if err := binary.Read(conn, binary.BigEndian, &infoType); err != nil {
					return fmt.Errorf("read info type: %w", err)
				}
				if infoType == NBDInfoBlockSize {
					wantBlockSize = true
				}
			}

			// Send NBD_INFO_BLOCK_SIZE if requested
			// This tells the client our alignment requirements
			if wantBlockSize {
				// Payload: info type (2) + min (4) + preferred (4) + max (4) = 14 bytes
				if err := WriteOptionReply(conn, optReq.Option, NBDRepInfo, 14); err != nil {
					return fmt.Errorf("write block size info reply: %w", err)
				}
				if err := binary.Write(conn, binary.BigEndian, uint16(NBDInfoBlockSize)); err != nil {
					return fmt.Errorf("write block size info type: %w", err)
				}
				// Minimum block size: 1 (we support any alignment)
				if err := binary.Write(conn, binary.BigEndian, uint32(1)); err != nil {
					return fmt.Errorf("write min block size: %w", err)
				}
				// Preferred block size: 4096
				if err := binary.Write(conn, binary.BigEndian, uint32(4096)); err != nil {
					return fmt.Errorf("write preferred block size: %w", err)
				}
				// Maximum block size: 32MB
				if err := binary.Write(conn, binary.BigEndian, uint32(32*1024*1024)); err != nil {
					return fmt.Errorf("write max block size: %w", err)
				}
			}

			// Send NBD_INFO_EXPORT
			if err := WriteOptionReply(conn, optReq.Option, NBDRepInfo, 12); err != nil {
				return fmt.Errorf("write info reply: %w", err)
			}
			// Info type: NBD_INFO_EXPORT = 0
			if err := binary.Write(conn, binary.BigEndian, uint16(NBDInfoExport)); err != nil {
				return fmt.Errorf("write info type: %w", err)
			}
			// Size
			if err := binary.Write(conn, binary.BigEndian, uint64(s.device.Size())); err != nil {
				return fmt.Errorf("write size: %w", err)
			}
			// Transmission flags
			transFlags := uint16(NBDFlagHasFlags | NBDFlagSendFlush | NBDFlagSendTrim)
			if err := binary.Write(conn, binary.BigEndian, transFlags); err != nil {
				return fmt.Errorf("write transmission flags: %w", err)
			}

			if optReq.Option == NBDOptGo {
				// Send ACK to complete negotiation
				if err := WriteOptionReply(conn, optReq.Option, NBDRepAck, 0); err != nil {
					return fmt.Errorf("write ack: %w", err)
				}
				log.Printf("Export negotiated (GO): name=%q size=%d", string(exportName), s.device.Size())
				return nil
			}
			// For INFO, send ACK and continue haggling
			if err := WriteOptionReply(conn, optReq.Option, NBDRepAck, 0); err != nil {
				return fmt.Errorf("write ack: %w", err)
			}

		case NBDOptList:
			// Discard any data
			if optReq.Length > 0 {
				discard := make([]byte, optReq.Length)
				if _, err := io.ReadFull(conn, discard); err != nil {
					return fmt.Errorf("discard list data: %w", err)
				}
			}
			// Send our single export
			exportNameBytes := []byte(s.exportName)
			replyLen := uint32(4 + len(exportNameBytes))
			if err := WriteOptionReply(conn, optReq.Option, NBDRepServer, replyLen); err != nil {
				return fmt.Errorf("write list reply: %w", err)
			}
			if err := binary.Write(conn, binary.BigEndian, uint32(len(exportNameBytes))); err != nil {
				return fmt.Errorf("write export name len: %w", err)
			}
			if _, err := conn.Write(exportNameBytes); err != nil {
				return fmt.Errorf("write export name: %w", err)
			}
			// Send ACK to end list
			if err := WriteOptionReply(conn, optReq.Option, NBDRepAck, 0); err != nil {
				return fmt.Errorf("write list ack: %w", err)
			}

		case NBDOptAbort:
			// Client wants to abort
			if err := WriteOptionReply(conn, optReq.Option, NBDRepAck, 0); err != nil {
				return fmt.Errorf("write abort ack: %w", err)
			}
			return errors.New("client aborted")

		default:
			// Unknown option - send unsupported error
			if optReq.Length > 0 {
				discard := make([]byte, optReq.Length)
				if _, err := io.ReadFull(conn, discard); err != nil {
					return fmt.Errorf("discard unknown option data: %w", err)
				}
			}
			if err := WriteOptionReply(conn, optReq.Option, NBDRepErrUnsup, 0); err != nil {
				return fmt.Errorf("write unsup reply: %w", err)
			}
		}
	}
}

// replyMsg is a message to send to the reply writer goroutine.
type replyMsg struct {
	handle  uint64
	errCode uint32
	data    []byte // for read responses
}

// handleCommands processes NBD commands from the client.
// Commands are dispatched asynchronously and replies are sent via a dedicated writer goroutine.
func (s *Server) handleCommands(ctx context.Context, conn net.Conn) error {
	return s.handleCommandsWithConnID(ctx, conn, -1)
}

// handleCommandsWithConnID processes NBD commands with connection ID for logging.
func (s *Server) handleCommandsWithConnID(_ context.Context, conn net.Conn, connID int) error {
	// Channel for replies
	replyCh := make(chan replyMsg, 64)

	// Track in-flight operations
	var inflightWg sync.WaitGroup

	// Start reply writer goroutine
	var replyWg sync.WaitGroup
	replyWg.Add(1)
	go func() {
		defer replyWg.Done()
		s.replyWriter(conn, replyCh)
	}()

	// Ensure reply writer is cleaned up
	defer func() {
		// Wait for all in-flight operations to complete before closing channel
		inflightWg.Wait()
		close(replyCh)
		replyWg.Wait()
	}()

	for {
		req, err := ReadRequest(conn)
		if err != nil {
			return err
		}

		if req.Magic != NBDRequestMagic {
			return fmt.Errorf("invalid request magic: %x", req.Magic)
		}

		switch req.Type {
		case NBDCmdRead:
			resultCh := s.device.SubmitRead(req.Handle, int(req.Length), int64(req.Offset))
			inflightWg.Add(1)
			go func() {
				defer inflightWg.Done()
				s.forwardReadReply(replyCh, req, resultCh)
			}()

		case NBDCmdWrite:
			// Read write data from connection (must be done before next request)
			data := make([]byte, req.Length)
			if _, err := io.ReadFull(conn, data); err != nil {
				return fmt.Errorf("read write data: %w", err)
			}
			// Submit takes ownership of data — do not use data after this point
			var resultCh <-chan OpResult
			if req.Flags&NBDCmdFlagFUA != 0 {
				// FUA: write must be durable before reply (only waits for this write)
				resultCh = s.device.SubmitWriteFUA(req.Handle, data, int64(req.Offset))
			} else {
				resultCh = s.device.SubmitWrite(req.Handle, data, int64(req.Offset))
			}
			data = nil //nolint:ineffassign // ownership transferred
			inflightWg.Add(1)
			go func() {
				defer inflightWg.Done()
				s.forwardReply(replyCh, req, resultCh)
			}()

		case NBDCmdDisc:
			log.Printf("Client requested disconnect")
			return nil

		case NBDCmdFlush:
			resultCh := s.device.SubmitFlush(req.Handle)
			inflightWg.Add(1)
			go func() {
				defer inflightWg.Done()
				s.forwardFlushReply(replyCh, req, resultCh)
			}()

		case NBDCmdTrim:
			resultCh := s.device.SubmitTrim(req.Handle, int64(req.Offset), int64(req.Length))
			inflightWg.Add(1)
			go func() {
				defer inflightWg.Done()
				s.forwardReply(replyCh, req, resultCh)
			}()

		case NBDCmdWriteZeros:
			// Write zeros - use trim (hole) which reads back as zeros
			// This is more efficient than actually writing zero bytes
			var resultCh <-chan OpResult
			if req.Flags&NBDCmdFlagFUA != 0 {
				// FUA: trim must be durable before reply
				resultCh = s.device.SubmitTrimFUA(req.Handle, int64(req.Offset), int64(req.Length))
			} else {
				resultCh = s.device.SubmitTrim(req.Handle, int64(req.Offset), int64(req.Length))
			}
			inflightWg.Add(1)
			go func() {
				defer inflightWg.Done()
				s.forwardReply(replyCh, req, resultCh)
			}()

		default:
			log.Printf("Unknown command type: %d", req.Type)
			replyCh <- replyMsg{handle: req.Handle, errCode: NBDErrInval}
		}
	}
}

// replyWriter writes replies to the connection.
// This serializes all writes to the connection.
func (s *Server) replyWriter(conn net.Conn, replyCh <-chan replyMsg) {
	for reply := range replyCh {
		if err := WriteSimpleReply(conn, reply.handle, reply.errCode); err != nil {
			log.Printf("Error writing reply: %v", err)
			return
		}
		if reply.data != nil && reply.errCode == NBDErrNone {
			if _, err := conn.Write(reply.data); err != nil {
				log.Printf("Error writing reply data: %v", err)
				return
			}
		}
	}
}

// forwardReply waits for an operation result and sends the reply.
func (s *Server) forwardReply(replyCh chan<- replyMsg, req *Request, resultCh <-chan OpResult) {
	result := <-resultCh

	var errCode uint32 = NBDErrNone
	if result.Err != nil {
		log.Printf("Operation error at offset %d: %v", req.Offset, result.Err)
		errCode = NBDErrIO
	}

	replyCh <- replyMsg{handle: req.Handle, errCode: errCode}
}

// forwardReadReply waits for a read result and sends the reply with data.
func (s *Server) forwardReadReply(replyCh chan<- replyMsg, req *Request, resultCh <-chan OpResult) {
	result := <-resultCh

	var errCode uint32 = NBDErrNone
	if result.Err != nil {
		log.Printf("Read error at offset %d: %v", req.Offset, result.Err)
		errCode = NBDErrIO
	}

	replyCh <- replyMsg{handle: req.Handle, errCode: errCode, data: result.Data}
}

// forwardFlushReply waits for a flush result and sends the reply.
// Also calls the onFlush callback if configured.
func (s *Server) forwardFlushReply(replyCh chan<- replyMsg, req *Request, resultCh <-chan OpResult) {
	result := <-resultCh

	var errCode uint32 = NBDErrNone
	if result.Err != nil {
		log.Printf("Flush error: %v", result.Err)
		errCode = NBDErrIO
	}

	replyCh <- replyMsg{handle: req.Handle, errCode: errCode}

	// Trigger callback after responding to client
	if errCode == NBDErrNone && s.onFlush != nil {
		s.onFlush()
	}
}
