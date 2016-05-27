# Torus release guide

This (evolving) guide outlines the exact steps necessary to do a release.

## Name the version

```
export VERSION=v0.1.0
```

## Tag and push the tag

```
git tag $VERSION
git push origin $VERSION
```

## Build release binaries

`make` respects the VERSION environment variable. You'll need [goxc](https://github.com/laher/goxc)

```
make release
```

## Publish release on Github

- Set release title as the version name.
- Follow the format of previous release pages.
- Attach the generated `.tar.gz`s
- Select whether it is a pre-release.
- Publish the release!

## Tag release on Quay.io
