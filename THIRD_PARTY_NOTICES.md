# Third-party notices

Nebula is licensed under Apache-2.0 (see [LICENSE](LICENSE)). It relies on the
following third-party software, which is licensed separately.

## Tailscale

The optional [SandD](https://github.com/InftyAI/SandD) access channel uses the
[Tailscale](https://github.com/tailscale/tailscale) client (`tailscale`/`tailscaled`),
© Tailscale Inc., licensed under
[BSD-3-Clause](https://github.com/tailscale/tailscale/blob/main/LICENSE).

Nebula does not link or vendor it; the provider integration fetches the official
binary onto the GPU host at provision time. If you build and redistribute an image
with Tailscale baked in, retain its copyright notice per BSD-3-Clause.
