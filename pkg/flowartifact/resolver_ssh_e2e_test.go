package flowartifact

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	cryptossh "golang.org/x/crypto/ssh"
)

// TestResolveGitOverSSHEndToEnd clones a real Git repository over SSH from an in-process SSH
// server that serves git-upload-pack, proving the SSH transport, public-key auth, and
// known_hosts verification work end to end — and that a mismatched host key is rejected.
func TestResolveGitOverSSHEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("git-upload-pack"); err != nil {
		t.Skip("git-upload-pack not available")
	}

	// A Git repo with a flow snapshot on the default branch.
	repoDir := t.TempDir()
	initGitRepoWithFlow(t, repoDir, []byte(`{"flowContents":{"name":"ssh-flow"}}`))

	clientKeyPEM, clientPub := generateSSHClientKey(t)
	hostSigner, hostPub := generateSSHHostKey(t)
	addr := startSSHGitServer(t, hostSigner, clientPub)
	knownHosts := []byte(knownHostsLine(knownHostsAddress(addr), hostPub))

	// scp-style URL requires an absolute path prefixed with the repo root; use ssh:// with the
	// absolute repo path so git-upload-pack receives it verbatim.
	url := fmt.Sprintf("ssh://git@%s%s", addr, repoDir)

	resolver := DefaultResolver{}
	artifact, err := resolver.resolveGit(context.Background(), nifiv1alpha1.GitSource{URL: url}, Credentials{
		SSHPrivateKey: clientKeyPEM,
		SSHKnownHosts: knownHosts,
	})
	if err != nil {
		t.Fatalf("SSH clone failed: %v", err)
	}
	if artifact == nil || !strings.Contains(string(artifact.Snapshot.Raw), "ssh-flow") {
		t.Fatalf("unexpected snapshot: %s", string(artifact.Snapshot.Raw))
	}
	if artifact.Revision == "" {
		t.Fatal("expected a resolved commit revision")
	}

	// A known_hosts entry with the wrong host key must fail verification.
	_, otherPub := generateSSHHostKey(t)
	wrongKnownHosts := []byte(knownHostsLine(knownHostsAddress(addr), otherPub))
	if _, err := resolver.resolveGit(context.Background(), nifiv1alpha1.GitSource{URL: url}, Credentials{
		SSHPrivateKey: clientKeyPEM,
		SSHKnownHosts: wrongKnownHosts,
	}); err == nil {
		t.Fatal("expected host-key verification to reject a mismatched host key")
	}
}

func generateSSHClientKey(t *testing.T) (pemBytes []byte, publicKey cryptossh.PublicKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	signer, err := cryptossh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pemBytes, signer.PublicKey()
}

func initGitRepoWithFlow(t *testing.T, dir string, flow []byte) {
	t.Helper()
	run := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	run("init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "flow.json"), flow, 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", "flow.json")
	run("-c", "user.email=test@example.com", "-c", "user.name=test", "commit", "-q", "-m", "flow")
}

// knownHostsAddress renders the host:port in the bracketed form known_hosts uses for non-22 ports.
func knownHostsAddress(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return fmt.Sprintf("[%s]:%s", host, port)
}

// startSSHGitServer runs an in-process SSH server that authorizes clientPub and serves
// git-upload-pack for exec requests. It returns the listen address and stops on test cleanup.
func startSSHGitServer(t *testing.T, hostSigner cryptossh.Signer, clientPub cryptossh.PublicKey) string {
	t.Helper()
	authorized := string(cryptossh.MarshalAuthorizedKey(clientPub))
	config := &cryptossh.ServerConfig{
		PublicKeyCallback: func(_ cryptossh.ConnMetadata, key cryptossh.PublicKey) (*cryptossh.Permissions, error) {
			if string(cryptossh.MarshalAuthorizedKey(key)) == authorized {
				return &cryptossh.Permissions{}, nil
			}
			return nil, fmt.Errorf("unauthorized key")
		},
	}
	config.AddHostKey(hostSigner)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveSSHConn(conn, config)
		}
	}()
	return listener.Addr().String()
}

func serveSSHConn(conn net.Conn, config *cryptossh.ServerConfig) {
	defer conn.Close()
	serverConn, channels, requests, err := cryptossh.NewServerConn(conn, config)
	if err != nil {
		return
	}
	defer serverConn.Close()
	go cryptossh.DiscardRequests(requests)
	for newChannel := range channels {
		if newChannel.ChannelType() != "session" {
			newChannel.Reject(cryptossh.UnknownChannelType, "only sessions")
			continue
		}
		channel, sessionRequests, err := newChannel.Accept()
		if err != nil {
			continue
		}
		go serveSSHSession(channel, sessionRequests)
	}
}

func serveSSHSession(channel cryptossh.Channel, requests <-chan *cryptossh.Request) {
	for req := range requests {
		if req.Type != "exec" {
			req.Reply(false, nil)
			continue
		}
		var payload struct{ Command string }
		if err := cryptossh.Unmarshal(req.Payload, &payload); err != nil {
			req.Reply(false, nil)
			channel.Close()
			return
		}
		req.Reply(true, nil)
		exitStatus := runGitUploadPack(channel, payload.Command)
		channel.CloseWrite()
		channel.SendRequest("exit-status", false, cryptossh.Marshal(struct{ Status uint32 }{uint32(exitStatus)}))
		channel.Close()
		return
	}
}

// runGitUploadPack parses `git-upload-pack '<path>'` and serves it over the SSH channel.
func runGitUploadPack(channel cryptossh.Channel, command string) int {
	fields := strings.SplitN(command, " ", 2)
	if len(fields) != 2 || fields[0] != "git-upload-pack" {
		return 1
	}
	path := strings.Trim(strings.TrimSpace(fields[1]), "'")
	cmd := exec.Command("git-upload-pack", path)
	cmd.Stdin = channel
	cmd.Stdout = channel
	cmd.Stderr = channel.Stderr()
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode()
		}
		return 1
	}
	return 0
}
