# Distribution and releases

DynamicDataWeb is published as `@wieslawsoltes/dynamicdataweb`. Node 22 and 24 CI validates source tests, demo smoke tests, declaration fixtures, API inventory and actual installed ESM/CommonJS/TypeScript consumers. The standard package uses the application's RxJS 7.8.2-compatible peer; standalone browser bundles include their own RxJS.

## Prepare a release

1. Open a PR containing the intended changes, update the version in `package.json` and `package-lock.json`, update `CHANGELOG.md` and `docs/release-notes.md`, and update the sample's displayed version/package download link.
2. Wait for both CI matrix checks, review, and merge the PR to `main`.
3. The CI and distribution workflow builds an npm tarball, browser archive and complete showcase archive once; it records SHA-256 checksums and publishes GitHub Packages and an immutable GitHub release named `v<version>`.
4. GitHub Pages deploys the same verified demo artifact. The reusable npm workflow downloads the release tarball, verifies its checksum and installed consumers, then publishes those exact bytes with provenance.
5. The npm job verifies the public version, explicit distribution tag, full npm install package index, SHA-512 integrity, provenance metadata, downloaded consumers and a fresh anonymous package-name installation. It allows up to five minutes for registry metadata propagation.

Existing release assets and npm versions are never overwritten. Commits that retain a previously released version can deploy the sample but do not republish npm unless they identify that version's release commit. Create a new version for changed package bytes.

## npm authentication

Set the repository secret `NPM_TOKEN` to a granular npm token with read/write publish access for the `@wieslawsoltes` scope and bypass two-factor authentication enabled. The token is exposed only to the publish step as `NODE_AUTH_TOKEN`. Public verification requests do not use authentication. The reusable workflow uses the `npm` GitHub environment; configure any desired protection rules there.

Alternatively configure npm trusted publishing with this repository and the invoking workflow `.github/workflows/ci.yml`; the job has `id-token: write` and uses npm 11. Trusted-publisher identity depends on the caller of a reusable workflow. For manual invocations, configure the corresponding `npm-publish.yml` identity if using OIDC. The token works for both entry points.

GitHub Packages uses the automatic `GITHUB_TOKEN`, independently of the npm token. Install from npm with:

```sh
npm install @wieslawsoltes/dynamicdataweb rxjs
```

## Retry an interrupted publication

Rerun the failed npm job in the original CI run, or manually dispatch **Publish npm registry** with the existing tag, optional expected commit SHA, and `latest` or `next` distribution tag. If npm already contains identical release bytes, publication is skipped and verification runs again. If its immutable version contains different bytes, the workflow fails rather than accepting or replacing them. Authentication, transport failures and unexpected registry responses are not treated as absence.

Release artifacts are also directly installable:

```sh
npm install https://github.com/wieslawsoltes/DynamicDataWeb/releases/download/v0.1.0/wieslawsoltes-dynamicdataweb-0.1.0.tgz rxjs
```
