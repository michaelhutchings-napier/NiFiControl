package flowartifact

import (
	"archive/tar"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
)

const simpleSigningMediaType = types.MediaType("application/vnd.dev.cosign.simplesigning.v1+json")

func writeFlowImage(t *testing.T, ref name.Reference) v1.Hash {
	t.Helper()
	var layerContents bytes.Buffer
	tarWriter := tar.NewWriter(&layerContents)
	payload := []byte("flowContents:\n  name: Payments\n  processors: []\n")
	if err := tarWriter.WriteHeader(&tar.Header{Name: "flow.json", Mode: 0o600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	layer, err := tarball.LayerFromReader(bytes.NewReader(layerContents.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	image, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, image); err != nil {
		t.Fatal(err)
	}
	digest, err := image.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

// pushCosignSignature builds a cosign simple-signing signature over manifestDigest with priv and
// pushes it to the conventional "<algo>-<hex>.sig" tag. When digestOverride is non-empty the payload
// attests to that digest instead, to exercise replay protection.
func pushCosignSignature(t *testing.T, repo name.Repository, manifestDigest v1.Hash, priv *ecdsa.PrivateKey, digestOverride string) {
	t.Helper()
	attested := manifestDigest.String()
	if digestOverride != "" {
		attested = digestOverride
	}
	document := map[string]any{
		"critical": map[string]any{
			"identity": map[string]any{"docker-reference": repo.Name()},
			"image":    map[string]any{"docker-manifest-digest": attested},
			"type":     "cosign container image signature",
		},
		"optional": nil,
	}
	payload, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(payload)
	signature, err := ecdsa.SignASN1(rand.Reader, priv, hash[:])
	if err != nil {
		t.Fatal(err)
	}
	layer := static.NewLayer(payload, simpleSigningMediaType)
	signatureImage, err := mutate.Append(empty.Image, mutate.Addendum{
		Layer:       layer,
		Annotations: map[string]string{cosignSignatureAnnotation: base64.StdEncoding.EncodeToString(signature)},
	})
	if err != nil {
		t.Fatal(err)
	}
	signatureRef := repo.Tag(fmt.Sprintf("%s-%s.sig", manifestDigest.Algorithm, manifestDigest.Hex))
	if err := remote.Write(signatureRef, signatureImage); err != nil {
		t.Fatal(err)
	}
}

func publicKeyPEM(t *testing.T, priv *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

func TestDefaultResolverVerifiesCosignSignature(t *testing.T) {
	server := httptest.NewServer(registry.New())
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")

	signer, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	other, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	resolver := DefaultResolver{AllowInsecureOCI: true}

	t.Run("valid signature is accepted", func(t *testing.T) {
		ref, err := name.ParseReference(host+"/flows/valid:v1", name.Insecure)
		if err != nil {
			t.Fatal(err)
		}
		digest := writeFlowImage(t, ref)
		pushCosignSignature(t, ref.Context(), digest, signer, "")

		artifact, err := resolver.Resolve(t.Context(), Request{
			Source:       nifiv1alpha1.FlowBundleSource{OCI: &nifiv1alpha1.OCISource{Image: ref.Name()}},
			Verification: &Verification{CosignPublicKeyPEM: publicKeyPEM(t, signer)},
		})
		if err != nil {
			t.Fatalf("expected a signed image to verify: %v", err)
		}
		var snapshot map[string]any
		if err := json.Unmarshal(artifact.Snapshot.Raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		if snapshot["flowContents"].(map[string]any)["name"] != "Payments" {
			t.Fatalf("snapshot = %#v", snapshot)
		}
	})

	t.Run("unsigned image is rejected", func(t *testing.T) {
		ref, err := name.ParseReference(host+"/flows/unsigned:v1", name.Insecure)
		if err != nil {
			t.Fatal(err)
		}
		writeFlowImage(t, ref)
		if _, err := resolver.Resolve(t.Context(), Request{
			Source:       nifiv1alpha1.FlowBundleSource{OCI: &nifiv1alpha1.OCISource{Image: ref.Name()}},
			Verification: &Verification{CosignPublicKeyPEM: publicKeyPEM(t, signer)},
		}); err == nil {
			t.Fatal("expected an unsigned image to be rejected")
		}
	})

	t.Run("signature by another key is rejected", func(t *testing.T) {
		ref, err := name.ParseReference(host+"/flows/wrongkey:v1", name.Insecure)
		if err != nil {
			t.Fatal(err)
		}
		digest := writeFlowImage(t, ref)
		pushCosignSignature(t, ref.Context(), digest, other, "")
		if _, err := resolver.Resolve(t.Context(), Request{
			Source:       nifiv1alpha1.FlowBundleSource{OCI: &nifiv1alpha1.OCISource{Image: ref.Name()}},
			Verification: &Verification{CosignPublicKeyPEM: publicKeyPEM(t, signer)},
		}); err == nil {
			t.Fatal("expected a signature made by a different key to be rejected")
		}
	})

	t.Run("signature for a different digest is rejected", func(t *testing.T) {
		ref, err := name.ParseReference(host+"/flows/replay:v1", name.Insecure)
		if err != nil {
			t.Fatal(err)
		}
		digest := writeFlowImage(t, ref)
		// Correctly signed payload, but attesting to a different manifest digest.
		pushCosignSignature(t, ref.Context(), digest, signer, "sha256:"+strings.Repeat("ab", 32))
		if _, err := resolver.Resolve(t.Context(), Request{
			Source:       nifiv1alpha1.FlowBundleSource{OCI: &nifiv1alpha1.OCISource{Image: ref.Name()}},
			Verification: &Verification{CosignPublicKeyPEM: publicKeyPEM(t, signer)},
		}); err == nil {
			t.Fatal("expected a signature attesting a different digest to be rejected")
		}
	})
}

func TestVerifyRawSignatureRejectsUnsupportedKey(t *testing.T) {
	if err := verifyRawSignature("not-a-key", []byte("payload"), []byte("sig")); err == nil {
		t.Fatal("expected unsupported key type to error")
	}
}

func TestParseVerificationPublicKeyRejectsNonPEM(t *testing.T) {
	if _, err := parseVerificationPublicKey([]byte("not pem")); err == nil {
		t.Fatal("expected non-PEM key to error")
	}
}
