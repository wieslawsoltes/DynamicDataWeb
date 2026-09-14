# Blazor publication and recovery

The build-source pin includes the bounded NuGet download verifier and its 14 offline regression tests. Component runtime APIs and already-published package versions are unchanged.

After upload, verification waits up to 720 seconds for public availability. It retries missing downloads, transient network failures, rate limits and selected server errors. Permanent errors and downloaded payload conflicts remain fatal. NUGET_VERIFY_TIMEOUT_SECONDS or --timeout-seconds overrides the budget; increase the publish job timeout when choosing a longer wait.

If upload succeeds but verification times out, rerun only the failed **publish** job from the original Actions run. Reuse its validated artifacts; do not rebuild, replace an existing version, or disable verification. Matching existing packages skip duplicate upload, but their complete payload is still compared before release creation. Only NuGet's root repository signature is excluded, not assemblies, assets or the nuspec.

Version.props versions NuGet independently of npm. Read [README.md](README.md) and [INTEGRATION.md](INTEGRATION.md) for build and hosting instructions.
