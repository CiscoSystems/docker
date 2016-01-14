package graph

import (
	"fmt"

	"github.com/docker/distribution/manifest"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/pkg/streamformatter"
	"github.com/docker/docker/registry"
	"github.com/docker/docker/utils"
)

// ManifestPuller is an interface that abstracts pulling manifests for different API versions.
type ManifestPuller interface {
	// Pull tries to pull the image referenced by `tag`
	// Pull returns an error if any, as well as a boolean that determines whether to retry Pull on the next configured endpoint.
	//
	Pull(tag string) (manifest *manifest.Manifest, fallback bool, err error)
}

// ManifestPusher is an interface that abstracts pushing manifests for different API versions.
type ManifestPusher interface {
	Push(manifest *manifest.Manifest) (fallback bool, err error)
}

// NewManifestPuller returns a Puller interface that will pull from v2
// registry. The endpoint argument contains a Version field that determines
// whether v2 puller will be created. The other parameters are passed
// through to the underlying puller implementation for use during the actual
// pull operation.
func NewManifestPuller(s *TagStore, endpoint registry.APIEndpoint, repoInfo *registry.RepositoryInfo, imagePullConfig *ImagePullConfig, sf *streamformatter.StreamFormatter) (ManifestPuller, error) {
	switch endpoint.Version {
	case registry.APIVersion2:
		return &v2ManifestPuller{
			TagStore: s,
			endpoint: endpoint,
			config:   imagePullConfig,
			sf:       sf,
			repoInfo: repoInfo,
		}, nil
	}
	return nil, fmt.Errorf("unknown version %d for registry %s", endpoint.Version, endpoint.URL)
}

// NewManifestPusher creates a new Pusher interface that will push to a v2
// registry. The endpoint argument contains a Version field that determines
// whether a v2 pusher will be created. The other parameters are passed
// through to the underlying pusher implementation for use during the actual
// push operation.
func (s *TagStore) NewManifestPusher(endpoint registry.APIEndpoint, repoInfo *registry.RepositoryInfo, imagePushConfig *ImagePushConfig, sf *streamformatter.StreamFormatter) (ManifestPusher, error) {
	switch endpoint.Version {
	case registry.APIVersion2:
		return &v2ManifestPusher{
			TagStore: s,
			endpoint: endpoint,
			repoInfo: repoInfo,
			config:   imagePushConfig,
			sf:       sf,
		}, nil
	}
	return nil, fmt.Errorf("unknown version %d for registry %s", endpoint.Version, endpoint.URL)
}

//RetagManifest pulls retags and pushes the new tag to the manifest
func (s *TagStore) RetagManifest(image string, tag string, newTag string, imagePullConfig *ImagePullConfig) error {
	logrus.Debugf("Image: %s\n", image)
	logrus.Debugf("Tag: %s\n", tag)
	logrus.Debugf("NewTag: %s\n", newTag)
	logrus.Debugf("imagePullConfig: %s\n", imagePullConfig)

	var sf = streamformatter.NewJSONStreamFormatter()

	// Resolve the Repository name from fqn to RepositoryInfo
	repoInfo, err := s.registryService.ResolveRepository(image)
	if err != nil {
		return err
	}
	logrus.Debugf("repoInfo: %s\n", repoInfo)
	// makes sure name is not empty or `scratch`
	if err := validateRepoName(repoInfo.LocalName); err != nil {
		return err
	}

	endpoints, err := s.registryService.LookupPullEndpoints(repoInfo.CanonicalName)
	if err != nil {
		return err
	}

	logName := repoInfo.LocalName
	if tag != "" {
		logName = utils.ImageReference(logName, tag)
	}
	logrus.Debugf("logName: %s\n", logName)
	var (
		lastErr error

		// discardNoSupportErrors is used to track whether an endpoint encountered an error of type registry.ErrNoSupport
		// By default it is false, which means that if a ErrNoSupport error is encountered, it will be saved in lastErr.
		// As soon as another kind of error is encountered, discardNoSupportErrors is set to true, avoiding the saving of
		// any subsequent ErrNoSupport errors in lastErr.
		// It's needed for pull-by-digest on v1 endpoints: if there are only v1 endpoints configured, the error should be
		// returned and displayed, but if there was a v2 endpoint which supports pull-by-digest, then the last relevant
		// error is the ones from v2 endpoints not v1.
		discardNoSupportErrors bool
		verifiedManifest       *manifest.Manifest
		fallback               bool
	)
	for _, endpoint := range endpoints {
		logrus.Debugf("Trying to pull %s from %s %s", repoInfo.LocalName, endpoint.URL, endpoint.Version)

		puller, err := NewManifestPuller(s, endpoint, repoInfo, imagePullConfig, sf)
		if err != nil {
			lastErr = err
			continue
		}
		verifiedManifest, fallback, err = puller.Pull(tag)
		if err != nil {
			if fallback {
				if _, ok := err.(registry.ErrNoSupport); !ok {
					// Because we found an error that's not ErrNoSupport, discard all subsequent ErrNoSupport errors.
					discardNoSupportErrors = true
					// save the current error
					lastErr = err
				} else if !discardNoSupportErrors {
					// Save the ErrNoSupport error, because it's either the first error or all encountered errors
					// were also ErrNoSupport errors.
					lastErr = err
				}
				continue
			}
			logrus.Debugf("Not continuing with error: %v", err)
			return err

		}
		logrus.Debugf("RetagManifest - verifiedManifest.Name: %s\n", verifiedManifest.Name)
		logrus.Debugf("RetagManifest - verifiedManifest.Tag: %s\n", verifiedManifest.Tag)
		logrus.Debugf("RetagManifest - verifiedManifest.Architecture: %s\n", verifiedManifest.Architecture)
		logrus.Debugf("RetagManifest - verifiedManifest.FSLayers: %s\n", verifiedManifest.FSLayers)
		logrus.Debugf("RetagManifest - verifiedManifest.History: %s\n", verifiedManifest.History)
		verifiedManifest.Tag = newTag
		logrus.Debugf("RetagManifest - Changed verifiedManifest.Tag: %s\n", verifiedManifest.Tag)

		endpoints, err := s.registryService.LookupPushEndpoints(repoInfo.CanonicalName)
		if err != nil {
			return err
		}
		imagePushConfig := &ImagePushConfig{
			MetaHeaders: imagePullConfig.MetaHeaders,
			AuthConfig:  imagePullConfig.AuthConfig,
			Tag:         verifiedManifest.Tag,
			OutStream:   imagePullConfig.OutStream,
		}
		imagePushConfig.OutStream.Write(sf.FormatStatus("", "The push refers to a repository [%s]", repoInfo.CanonicalName))
		for _, endpoint := range endpoints {
			logrus.Debugf("Trying to push %s to %s %s", repoInfo.CanonicalName, endpoint.URL, endpoint.Version)

			pusher, err := s.NewManifestPusher(endpoint, repoInfo, imagePushConfig, sf)
			if err != nil {
				lastErr = err
				continue
			}
			if fallback, err := pusher.Push(verifiedManifest); err != nil {
				if fallback {
					lastErr = err
					continue
				}
				logrus.Debugf("Not continuing with error: %v", err)
				return err

			}

			s.eventsService.Log("push", repoInfo.LocalName, "")
			return nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no endpoints found for %s", image)
	}
	return lastErr
}
