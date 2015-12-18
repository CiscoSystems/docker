package graph

import (
	"fmt"
	"io"

	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest"
	"github.com/docker/docker/pkg/streamformatter"
	"github.com/docker/docker/pkg/stringid"
	"github.com/docker/docker/registry"
	"github.com/docker/docker/utils"
	"golang.org/x/net/context"
)

type v2ManifestPuller struct {
	*TagStore
	endpoint  registry.APIEndpoint
	config    *ImagePullConfig
	sf        *streamformatter.StreamFormatter
	repoInfo  *registry.RepositoryInfo
	repo      distribution.Repository
	sessionID string
}

type v2ManifestPusher struct {
	*TagStore
	endpoint registry.APIEndpoint
	repoInfo *registry.RepositoryInfo
	config   *ImagePushConfig
	sf       *streamformatter.StreamFormatter
	repo     distribution.Repository
}

func (p *v2ManifestPuller) Pull(tag string) (verifiedManifest *manifest.Manifest, fallback bool, err error) {
	fmt.Println("v2ManifestPuller.Pull")
	// TODO(tiborvass): was ReceiveTimeout
	p.repo, err = NewV2Repository(p.repoInfo, p.endpoint, p.config.MetaHeaders, p.config.AuthConfig, "pull")
	if err != nil {
		logrus.Warnf("Error getting v2 registry: %v", err)
		return nil, true, err
	}

	p.sessionID = stringid.GenerateRandomID()

	verifiedManifest, err = p.pullV2Manifest(tag)
	if err != nil {
		if registry.ContinueOnError(err) {
			logrus.Debugf("Error trying v2 registry: %v", err)
			return nil, true, err
		}
		return nil, false, err
	}

	return verifiedManifest, false, nil
}

func (p *v2ManifestPuller) pullV2Manifest(tag string) (verifiedManifest *manifest.Manifest, err error) {
	fmt.Println("v2ManifestPuller.pullV2Manifest")
	var tags []string
	taggedName := p.repoInfo.LocalName
	if len(tag) > 0 {
		tags = []string{tag}
		taggedName = utils.ImageReference(p.repoInfo.LocalName, tag)
	} else {
		var err error

		manSvc, err := p.repo.Manifests(context.Background())
		if err != nil {
			return nil, err
		}

		tags, err = manSvc.Tags()
		if err != nil {
			return nil, err
		}

	}
	poolKey := "v2:" + taggedName
	broadcaster, found := p.poolAdd("pull", poolKey)
	broadcaster.Add(p.config.OutStream)
	if found {
		// Another pull of the same repository is already taking place; just wait for it to finish
		return nil, broadcaster.Wait()
	}

	// This must use a closure so it captures the value of err when the
	// function returns, not when the 'defer' is evaluated.
	defer func() {
		p.poolRemoveWithError("pull", poolKey, err)
	}()

	var layersDownloaded bool

	for _, tag := range tags {
		// pulledNew is true if either new layers were downloaded OR if existing images were newly tagged
		// TODO(tiborvass): should we change the name of `layersDownload`? What about message in WriteStatus?
		pulledNew, pulledManifest, err := p.pullV2ManifestTag(broadcaster, tag, taggedName)
		if err != nil {
			return nil, err
		}
		verifiedManifest = pulledManifest
		layersDownloaded = layersDownloaded || pulledNew
	}

	writeStatus(taggedName, broadcaster, p.sf, layersDownloaded)

	return verifiedManifest, nil
}

func (p *v2ManifestPuller) pullV2ManifestTag(out io.Writer, tag, taggedName string) (tagUpdated bool, verifiedManifest *manifest.Manifest, err error) {
	logrus.Debugf("Pulling tag from V2 registry: %q", tag)

	manSvc, err := p.repo.Manifests(context.Background())
	if err != nil {
		return false, nil, err
	}

	unverifiedManifest, err := manSvc.GetByTag(tag)
	if err != nil {
		return false, nil, err
	}
	if unverifiedManifest == nil {
		return false, nil, fmt.Errorf("image manifest does not exist for tag %q", tag)
	}
	fmt.Printf("unverifiedManifest: %s\n", unverifiedManifest)
	verifiedManifest, err = verifyManifest(unverifiedManifest, tag)
	if err != nil {
		return false, nil, err
	}
	fmt.Printf("verifiedManifest: %s\n", verifiedManifest)

	// remove duplicate layers and check parent chain validity
	err = fixManifestLayers(verifiedManifest)
	if err != nil {
		return false, nil, err
	}

	manifestDigest, _, err := digestFromManifest(unverifiedManifest, p.repoInfo.LocalName)
	if err != nil {
		return false, nil, err
	}
	fmt.Printf("manifestDigest: %s\n", manifestDigest)

	return true, verifiedManifest, nil
}
func (p *v2ManifestPusher) Push(verifiedManifest *manifest.Manifest) (fallback bool, err error) {
	fmt.Println("v2ManifestPusher.Push")
	p.repo, err = NewV2Repository(p.repoInfo, p.endpoint, p.config.MetaHeaders, p.config.AuthConfig, "push", "pull")
	if err != nil {
		logrus.Debugf("Error getting v2 registry: %v", err)
		return true, err
	}
	return false, p.pushV2Manifest(verifiedManifest)
}

func (p *v2ManifestPusher) pushV2Manifest(m *manifest.Manifest) error {
	fmt.Println("v2ManifestPusher.PushV2Manifest")
	localName := p.repoInfo.LocalName
	if _, found := p.poolAdd("push", localName); found {
		return fmt.Errorf("push or pull %s is already in progress", localName)
	}
	defer p.poolRemove("push", localName)

	signed, err := manifest.Sign(m, p.trustKey)
	if err != nil {
		return err
	}

	manifestDigest, manifestSize, err := digestFromManifest(signed, p.repo.Name())
	if err != nil {
		return err
	}
	if manifestDigest != "" {
		p.config.OutStream.Write(p.sf.FormatStatus("", "%s: digest: %s size: %d", m.Tag, manifestDigest, manifestSize))
	}

	manSvc, err := p.repo.Manifests(context.Background())
	if err != nil {
		return err
	}
	return manSvc.Put(signed)
}
