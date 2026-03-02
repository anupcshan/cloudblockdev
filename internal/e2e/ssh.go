package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"time"

	"golang.org/x/crypto/ssh"
)

// SSHKeyPair contains an SSH key pair for VM authentication.
type SSHKeyPair struct {
	PrivateKey []byte // PEM-encoded private key
	PublicKey  string // OpenSSH authorized_keys format
	Signer     ssh.Signer
}

// GenerateSSHKeyPair creates a new Ed25519 SSH key pair.
func GenerateSSHKeyPair() (*SSHKeyPair, error) {
	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating ed25519 key: %w", err)
	}

	// Encode private key to PEM
	privKeyBytes, err := ssh.MarshalPrivateKey(privKey, "")
	if err != nil {
		return nil, fmt.Errorf("marshaling private key: %w", err)
	}
	privKeyPEM := pem.EncodeToMemory(privKeyBytes)

	// Create SSH signer
	signer, err := ssh.NewSignerFromKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("creating signer: %w", err)
	}

	// Format public key for authorized_keys
	sshPubKey, err := ssh.NewPublicKey(pubKey)
	if err != nil {
		return nil, fmt.Errorf("creating ssh public key: %w", err)
	}
	authorizedKey := string(ssh.MarshalAuthorizedKey(sshPubKey))

	return &SSHKeyPair{
		PrivateKey: privKeyPEM,
		PublicKey:  authorizedKey,
		Signer:     signer,
	}, nil
}

// SaveSSHKeyPair saves an SSH key pair to disk.
// Creates two files: basePath (private key) and basePath.pub (public key).
func SaveSSHKeyPair(kp *SSHKeyPair, basePath string) error {
	// Save private key
	if err := os.WriteFile(basePath, kp.PrivateKey, 0600); err != nil {
		return fmt.Errorf("writing private key: %w", err)
	}

	// Save public key
	if err := os.WriteFile(basePath+".pub", []byte(kp.PublicKey), 0644); err != nil {
		return fmt.Errorf("writing public key: %w", err)
	}

	return nil
}

// LoadSSHKeyPair loads an SSH key pair from disk.
func LoadSSHKeyPair(basePath string) (*SSHKeyPair, error) {
	// Read private key
	privKeyPEM, err := os.ReadFile(basePath)
	if err != nil {
		return nil, fmt.Errorf("reading private key: %w", err)
	}

	// Read public key
	pubKeyBytes, err := os.ReadFile(basePath + ".pub")
	if err != nil {
		return nil, fmt.Errorf("reading public key: %w", err)
	}

	// Parse private key to create signer
	signer, err := ssh.ParsePrivateKey(privKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("parsing private key: %w", err)
	}

	return &SSHKeyPair{
		PrivateKey: privKeyPEM,
		PublicKey:  string(pubKeyBytes),
		Signer:     signer,
	}, nil
}

// SSHClient wraps an SSH connection with helper methods.
type SSHClient struct {
	client *ssh.Client
}

// DialSSH connects to an SSH server with retries.
func DialSSH(ctx context.Context, addr string, signer ssh.Signer, user string) (*SSHClient, error) {
	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // OK for ephemeral test VMs
		Timeout:         10 * time.Second,
	}

	var client *ssh.Client
	var lastErr error

	// Retry connection until context is cancelled
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return nil, fmt.Errorf("SSH connection failed (last error: %v): %w", lastErr, ctx.Err())
			}
			return nil, ctx.Err()
		case <-ticker.C:
			var err error
			client, err = ssh.Dial("tcp", addr, config)
			if err == nil {
				return &SSHClient{client: client}, nil
			}
			lastErr = err
		}
	}
}

// Run executes a command and returns stdout, stderr, and any error.
func (c *SSHClient) Run(ctx context.Context, cmd string) (stdout, stderr string, err error) {
	session, err := c.client.NewSession()
	if err != nil {
		return "", "", fmt.Errorf("creating session: %w", err)
	}
	defer session.Close()

	var stdoutBuf, stderrBuf bytes.Buffer
	session.Stdout = &stdoutBuf
	session.Stderr = &stderrBuf

	// Run command with context cancellation
	done := make(chan error, 1)
	go func() {
		done <- session.Run(cmd)
	}()

	select {
	case <-ctx.Done():
		session.Signal(ssh.SIGKILL)
		return stdoutBuf.String(), stderrBuf.String(), ctx.Err()
	case err := <-done:
		return stdoutBuf.String(), stderrBuf.String(), err
	}
}

// RunWithSudo executes a command with sudo.
func (c *SSHClient) RunWithSudo(ctx context.Context, cmd string) (stdout, stderr string, err error) {
	return c.Run(ctx, "sudo "+cmd)
}

// Close closes the SSH connection.
func (c *SSHClient) Close() error {
	return c.client.Close()
}

// WaitForSSH waits for SSH to become available on the given address.
func WaitForSSH(ctx context.Context, addr string, signer ssh.Signer, user string) (*SSHClient, error) {
	return DialSSH(ctx, addr, signer, user)
}

// IsPortOpen checks if a TCP port is accepting connections.
func IsPortOpen(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
