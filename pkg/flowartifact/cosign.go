package flowartifact

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// cosignSignatureAnnotation is the manifest-layer annotation cosign stores a base64 signature under.
const cosignSignatureAnnotation = "dev.cosignproject.cosign/signature"

// maxCosignPayloadBytes bounds the simple-signing payload read from a signature layer.
const maxCosignPayloadBytes = 1 << 20

// verifyCosignSignature verifies that the image identified by digest carries a valid cosign
// key-based ("simple signing") signature made by publicKeyPEM. It follows cosign's convention of
// storing signatures in a companion image tagged "<algorithm>-<hex>.sig" in the same repository:
// each signature layer's payload is the signed simple-signing document and the base64 signature is
// on the layer's annotation. A signature is accepted only when the public key verifies the payload
// and the payload's docker-manifest-digest matches the image being resolved. Keyless (Fulcio/Rekor)
// signatures are not supported.
func verifyCosignSignature(ctx context.Context, repo name.Repository, digest v1.Hash, publicKeyPEM []byte, remoteOptions []remote.Option) error {
	publicKey, err := parseVerificationPublicKey(publicKeyPEM)
	if err != nil {
		return err
	}
	signatureRef := repo.Tag(fmt.Sprintf("%s-%s.sig", digest.Algorithm, digest.Hex))
	options := append([]remote.Option{remote.WithContext(ctx)}, remoteOptions...)
	signatureImage, err := remote.Image(signatureRef, options...)
	if err != nil {
		return fmt.Errorf("fetch cosign signature %q: %w", signatureRef.Identifier(), err)
	}
	manifest, err := signatureImage.Manifest()
	if err != nil {
		return fmt.Errorf("read cosign signature manifest: %w", err)
	}

	want := digest.String()
	lastErr := errors.New("no cosign signatures are attached")
	for _, descriptor := range manifest.Layers {
		encoded, ok := descriptor.Annotations[cosignSignatureAnnotation]
		if !ok || encoded == "" {
			continue
		}
		signature, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			lastErr = fmt.Errorf("decode cosign signature: %w", err)
			continue
		}
		layer, err := signatureImage.LayerByDigest(descriptor.Digest)
		if err != nil {
			lastErr = err
			continue
		}
		payload, err := cosignSignaturePayload(layer)
		if err != nil {
			lastErr = err
			continue
		}
		if err := verifyRawSignature(publicKey, payload, signature); err != nil {
			lastErr = err
			continue
		}
		if err := cosignPayloadMatchesDigest(payload, want); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("no valid cosign signature for %s: %w", want, lastErr)
}

func cosignSignaturePayload(layer v1.Layer) ([]byte, error) {
	reader, err := layer.Uncompressed()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(io.LimitReader(reader, maxCosignPayloadBytes))
}

// cosignPayloadMatchesDigest checks the simple-signing payload attests to the expected image digest,
// so a valid signature for a different image cannot be replayed for this one.
func cosignPayloadMatchesDigest(payload []byte, wantDigest string) error {
	var document struct {
		Critical struct {
			Image struct {
				DockerManifestDigest string `json:"docker-manifest-digest"`
			} `json:"image"`
		} `json:"critical"`
	}
	if err := json.Unmarshal(payload, &document); err != nil {
		return fmt.Errorf("decode cosign signature payload: %w", err)
	}
	if document.Critical.Image.DockerManifestDigest != wantDigest {
		return fmt.Errorf("cosign signature is for %q, not %q", document.Critical.Image.DockerManifestDigest, wantDigest)
	}
	return nil
}

func parseVerificationPublicKey(publicKeyPEM []byte) (crypto.PublicKey, error) {
	block, _ := pem.Decode(publicKeyPEM)
	if block == nil {
		return nil, errors.New("verification public key is not PEM encoded")
	}
	publicKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse verification public key: %w", err)
	}
	return publicKey, nil
}

// verifyRawSignature verifies a cosign signature over the exact payload bytes. ECDSA and RSA sign
// the SHA-256 digest of the payload; Ed25519 signs the payload directly. RSA accepts PSS or
// PKCS#1 v1.5 as cosign may emit either.
func verifyRawSignature(publicKey crypto.PublicKey, payload, signature []byte) error {
	switch key := publicKey.(type) {
	case *ecdsa.PublicKey:
		hash := sha256.Sum256(payload)
		if ecdsa.VerifyASN1(key, hash[:], signature) {
			return nil
		}
		return errors.New("ECDSA signature verification failed")
	case ed25519.PublicKey:
		if ed25519.Verify(key, payload, signature) {
			return nil
		}
		return errors.New("Ed25519 signature verification failed")
	case *rsa.PublicKey:
		hash := sha256.Sum256(payload)
		if rsa.VerifyPSS(key, crypto.SHA256, hash[:], signature, nil) == nil {
			return nil
		}
		if rsa.VerifyPKCS1v15(key, crypto.SHA256, hash[:], signature) == nil {
			return nil
		}
		return errors.New("RSA signature verification failed")
	default:
		return fmt.Errorf("unsupported verification public key type %T", publicKey)
	}
}
