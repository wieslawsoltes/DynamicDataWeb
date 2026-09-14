# DynamicDataWeb

Reactive keyed caches and ordered lists for JavaScript, TypeScript and Blazor, powered by RxJS.

[![npm](https://img.shields.io/npm/v/%40wieslawsoltes%2Fdynamicdataweb)](https://www.npmjs.com/package/@wieslawsoltes/dynamicdataweb)
[![npm downloads](https://img.shields.io/npm/dm/%40wieslawsoltes%2Fdynamicdataweb)](https://www.npmjs.com/package/@wieslawsoltes/dynamicdataweb)
[![NuGet](https://img.shields.io/nuget/v/DynamicDataWeb.Blazor)](https://www.nuget.org/packages/DynamicDataWeb.Blazor)
[![NuGet downloads](https://img.shields.io/nuget/dt/DynamicDataWeb.Blazor)](https://www.nuget.org/packages/DynamicDataWeb.Blazor)
[![Blazor CI](https://github.com/wieslawsoltes/DynamicDataWeb/actions/workflows/blazor.yml/badge.svg)](https://github.com/wieslawsoltes/DynamicDataWeb/actions/workflows/blazor.yml)

## JavaScript

```sh
npm install @wieslawsoltes/dynamicdataweb rxjs
```

The [complete JavaScript guide](README.web.md) retains all existing API examples, architecture, tests, compatibility information and notices. [Open the web demo](https://wieslawsoltes.github.io/DynamicDataWeb/).

## Blazor

```sh
dotnet add package DynamicDataWeb.Blazor --version 0.2.0
```

The .NET 8/.NET 10 package bundles the real engine and RxJS for interactive WebAssembly and Server, without a consumer npm/CDN dependency. It provides typed `SourceCache<T,TKey>` and `SourceList<T>`, full collection notifications, `DynamicDataProvider`, and `DynamicDataView<TItem>` with Razor rendering. Native object/function handles also expose advanced operators.

Read the [Blazor guide](blazor/README.md), [integration contract](blazor/INTEGRATION.md), [sample](blazor/sample/Demo.razor) and [release notes](blazor/RELEASE.md).

```sh
git submodule update --init --recursive
npm ci
npm run build
node blazor/build.mjs
dotnet run --project blazor/sample/Sample.csproj
# Or: dotnet run --project blazor/server/Server.csproj
```

Source builds require the .NET 10 SDK with .NET 8 targeting support. The Server sample uses `/probe/`. CI restores the actual nupkg into both target frameworks and hosts, testing native collections, notifications, streams, Razor callbacks and remounting.

`blazor/Version.props` owns NuGet versioning separately from npm. Version-changing main merges publish after validation using `NUGET_API_KEY` (`NUGET_TOKEN`/`NUGET_KEY` aliases), verify the public payload and create `blazor-v*` releases with symbols, runnable samples and checksums. Native API/browser boundaries remain applicable; this wrapper is not an exhaustive generated C# DynamicData port. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
